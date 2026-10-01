#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

// ONNX Runtime is NOT linked. The shared library comes from the `onnxruntime` pip package
// (`pip install opteryx-core[embeddings]`) and is loaded at runtime with dlopen; this file
// talks to it only through the stable C API (third_party/onnxruntime, headers only). The
// engine wheel therefore carries no ONNX Runtime code at all.
#include <dlfcn.h>
#include "onnxruntime_c_api.h"

// Draken vector ABI — this file produces a DrakenVector result for the embedding
// capability kernel below. The symbols (draken_malloc / draken_identity_sel) live in
// draken's extension and resolve at load time.
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/fp16.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "ops/kernels/kernel_context.h"
#include "ops/vec_result.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace {

// ---------------------------------------------------------------------------
// ONNX Runtime, loaded at runtime
// ---------------------------------------------------------------------------

const OrtApi* g_ort = nullptr;

void ort_check(OrtStatus* status, const char* what) {
    if (status == nullptr) return;
    std::string msg = std::string(what) + ": " + g_ort->GetErrorMessage(status);
    g_ort->ReleaseStatus(status);
    throw std::runtime_error(msg);
}

// Load the library once per process. A second install with a different path is refused:
// two ONNX Runtimes in one process would be two allocators and two thread pools.
void load_onnxruntime(const std::string& library_path) {
    static std::string loaded_path;
    if (g_ort != nullptr) {
        if (library_path != loaded_path)
            throw std::runtime_error("onnxruntime is already loaded from " + loaded_path +
                                     "; refusing to load a second copy from " + library_path);
        return;
    }
    void* handle = dlopen(library_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
        throw std::runtime_error("unable to load onnxruntime from " + library_path + ": " +
                                 dlerror());
    using get_api_base_t = const OrtApiBase* (*)();
    auto get_api_base = reinterpret_cast<get_api_base_t>(dlsym(handle, "OrtGetApiBase"));
    if (get_api_base == nullptr)
        throw std::runtime_error(library_path + " does not export OrtGetApiBase");
    const OrtApiBase* base = get_api_base();
    const OrtApi* api = base->GetApi(ORT_API_VERSION);
    if (api == nullptr)
        throw std::runtime_error(std::string("onnxruntime ") + base->GetVersionString() +
                                 " is older than the C API version 30 this engine was built "
                                 "against; install onnxruntime>=1.30");
    // Intentionally never dlclose'd: the kernel pointer handed to the registry lives for
    // the process, and so must the library behind it.
    g_ort = api;
    loaded_path = library_path;
}

// Owning handles: every ORT object is released through its own Release function.
struct EnvDel     { void operator()(OrtEnv* p) const noexcept { if (p) g_ort->ReleaseEnv(p); } };
struct SessDel    { void operator()(OrtSession* p) const noexcept { if (p) g_ort->ReleaseSession(p); } };
struct OptsDel    { void operator()(OrtSessionOptions* p) const noexcept { if (p) g_ort->ReleaseSessionOptions(p); } };
struct MemDel     { void operator()(OrtMemoryInfo* p) const noexcept { if (p) g_ort->ReleaseMemoryInfo(p); } };
struct ValDel     { void operator()(OrtValue* p) const noexcept { if (p) g_ort->ReleaseValue(p); } };
struct ShapeDel   { void operator()(OrtTensorTypeAndShapeInfo* p) const noexcept { if (p) g_ort->ReleaseTensorTypeAndShapeInfo(p); } };
struct TypeDel    { void operator()(OrtTypeInfo* p) const noexcept { if (p) g_ort->ReleaseTypeInfo(p); } };
using EnvPtr   = std::unique_ptr<OrtEnv, EnvDel>;
using SessPtr  = std::unique_ptr<OrtSession, SessDel>;
using OptsPtr  = std::unique_ptr<OrtSessionOptions, OptsDel>;
using MemPtr   = std::unique_ptr<OrtMemoryInfo, MemDel>;
using ValPtr   = std::unique_ptr<OrtValue, ValDel>;
using ShapePtr = std::unique_ptr<OrtTensorTypeAndShapeInfo, ShapeDel>;
using TypePtr  = std::unique_ptr<OrtTypeInfo, TypeDel>;

std::vector<int64_t> tensor_shape(const OrtTensorTypeAndShapeInfo* info) {
    size_t rank = 0;
    ort_check(g_ort->GetDimensionsCount(info, &rank), "GetDimensionsCount");
    std::vector<int64_t> dims(rank);
    ort_check(g_ort->GetDimensions(info, dims.data(), rank), "GetDimensions");
    return dims;
}

// ---------------------------------------------------------------------------
// WordPiece tokenizer (BERT uncased)
// ---------------------------------------------------------------------------

bool is_whitespace(unsigned char ch) { return std::isspace(ch) != 0; }
bool is_control(unsigned char ch) { return ch < 32 && ch != '\t' && ch != '\n' && ch != '\r'; }
bool is_punctuation(unsigned char ch) { return std::ispunct(ch) != 0; }

std::string normalize_text(std::string_view text) {
    std::string out;
    out.reserve(text.size());
    for (unsigned char ch : text) {
        if (is_control(ch)) continue;
        if (is_whitespace(ch)) { out.push_back(' '); continue; }
        out.push_back(static_cast<char>(std::tolower(ch)));
    }
    return out;
}

std::vector<std::string> basic_tokenize(std::string_view text) {
    std::vector<std::string> tokens;
    std::string current;
    current.reserve(32);
    for (unsigned char ch : text) {
        if (is_whitespace(ch)) {
            if (!current.empty()) { tokens.push_back(current); current.clear(); }
            continue;
        }
        if (is_punctuation(ch)) {
            if (!current.empty()) { tokens.push_back(current); current.clear(); }
            tokens.emplace_back(1, static_cast<char>(ch));
            continue;
        }
        current.push_back(static_cast<char>(ch));
    }
    if (!current.empty()) tokens.push_back(current);
    return tokens;
}

// ---------------------------------------------------------------------------
// MiniLM sentence embedder: tokenize -> ONNX forward pass -> mean pool -> L2 normalise
// ---------------------------------------------------------------------------

class MiniLMEmbedder {
  public:
    MiniLMEmbedder(const std::string& model_path, const std::string& vocab_path,
                   std::size_t max_length)
        : max_length_(max_length) {
        if (max_length_ < 3) throw std::runtime_error("max_length must be at least 3");
        load_vocab(vocab_path);

        OrtEnv* env = nullptr;
        ort_check(g_ort->CreateEnv(ORT_LOGGING_LEVEL_WARNING, "opteryx_minilm", &env), "CreateEnv");
        env_.reset(env);

        OrtSessionOptions* opts = nullptr;
        ort_check(g_ort->CreateSessionOptions(&opts), "CreateSessionOptions");
        OptsPtr opts_owner(opts);
        ort_check(g_ort->SetIntraOpNumThreads(opts, 1), "SetIntraOpNumThreads");
        ort_check(g_ort->SetSessionGraphOptimizationLevel(opts, ORT_ENABLE_ALL),
                  "SetSessionGraphOptimizationLevel");

        OrtSession* session = nullptr;
        ort_check(g_ort->CreateSession(env_.get(), model_path.c_str(), opts, &session),
                  "CreateSession");
        session_.reset(session);

        ort_check(g_ort->GetAllocatorWithDefaultOptions(&allocator_),
                  "GetAllocatorWithDefaultOptions");
        load_input_names();
        load_output();

        OrtMemoryInfo* mem = nullptr;
        ort_check(g_ort->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault, &mem),
                  "CreateCpuMemoryInfo");
        memory_info_.reset(mem);
    }

    std::size_t dimensions() const { return hidden_size_; }

    // Mean-pooled, L2-normalised fp32 rows, one per text. Thread-safe: OrtSession::Run is.
    std::vector<std::vector<float>> embed_texts(const std::vector<std::string>& texts) const {
        if (texts.empty()) return {};

        const std::size_t batch_size = texts.size();
        std::vector<std::vector<std::int64_t>> encoded_rows;
        encoded_rows.reserve(batch_size);
        std::size_t sequence_length = 0;
        for (const std::string& text : texts) {
            auto encoded = encode(text);
            sequence_length = std::max(sequence_length, encoded.size());
            encoded_rows.push_back(std::move(encoded));
        }

        std::vector<std::int64_t> input_ids(batch_size * sequence_length, pad_id_);
        std::vector<std::int64_t> attention_mask(batch_size * sequence_length, 0);
        std::vector<std::int64_t> token_type_ids(batch_size * sequence_length, 0);
        for (std::size_t row = 0; row < batch_size; ++row) {
            const auto& encoded = encoded_rows[row];
            for (std::size_t col = 0; col < encoded.size(); ++col) {
                input_ids[row * sequence_length + col] = encoded[col];
                attention_mask[row * sequence_length + col] = 1;
            }
        }

        const int64_t shape[2] = {static_cast<int64_t>(batch_size),
                                  static_cast<int64_t>(sequence_length)};
        std::vector<std::int64_t>* buffers[3] = {&input_ids, &attention_mask, &token_type_ids};
        ValPtr owned[3];
        const OrtValue* inputs[3] = {nullptr, nullptr, nullptr};
        for (std::size_t i = 0; i < input_name_ptrs_.size(); ++i) {
            OrtValue* v = nullptr;
            ort_check(g_ort->CreateTensorWithDataAsOrtValue(
                          memory_info_.get(), buffers[i]->data(),
                          buffers[i]->size() * sizeof(std::int64_t), shape, 2,
                          ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64, &v),
                      "CreateTensorWithDataAsOrtValue");
            owned[i].reset(v);
            inputs[i] = v;
        }

        OrtValue* out_raw = nullptr;
        const char* output_name = output_name_.c_str();
        ort_check(g_ort->Run(session_.get(), nullptr, input_name_ptrs_.data(), inputs,
                             input_name_ptrs_.size(), &output_name, 1, &out_raw),
                  "Run");
        ValPtr output(out_raw);

        int is_tensor = 0;
        ort_check(g_ort->IsTensor(output.get(), &is_tensor), "IsTensor");
        if (!is_tensor) throw std::runtime_error("MiniLM inference did not return a tensor");

        OrtTensorTypeAndShapeInfo* info_raw = nullptr;
        ort_check(g_ort->GetTensorTypeAndShape(output.get(), &info_raw), "GetTensorTypeAndShape");
        ShapePtr info(info_raw);
        const std::vector<int64_t> out_shape = tensor_shape(info.get());
        if (out_shape.size() != 3)
            throw std::runtime_error("MiniLM output tensor has unexpected rank");
        if (static_cast<std::size_t>(out_shape[0]) != batch_size ||
            static_cast<std::size_t>(out_shape[1]) != sequence_length ||
            static_cast<std::size_t>(out_shape[2]) != hidden_size_)
            throw std::runtime_error("MiniLM output tensor shape does not match its input");

        float* output_data = nullptr;
        ort_check(g_ort->GetTensorMutableData(output.get(), reinterpret_cast<void**>(&output_data)),
                  "GetTensorMutableData");

        std::vector<std::vector<float>> embeddings(batch_size, std::vector<float>(hidden_size_, 0.0f));
        for (std::size_t row = 0; row < batch_size; ++row) {
            float token_count = 0.0f;
            for (std::size_t col = 0; col < sequence_length; ++col) {
                if (attention_mask[row * sequence_length + col] == 0) continue;
                const std::size_t offset = (row * sequence_length + col) * hidden_size_;
                for (std::size_t dim = 0; dim < hidden_size_; ++dim)
                    embeddings[row][dim] += output_data[offset + dim];
                token_count += 1.0f;
            }
            // encode() always emits [CLS] and [SEP], so every row has tokens.
            float norm = 0.0f;
            for (float& value : embeddings[row]) {
                value /= token_count;
                norm += value * value;
            }
            norm = std::sqrt(norm);
            if (norm > 0.0f)
                for (float& value : embeddings[row]) value /= norm;
        }
        return embeddings;
    }

  private:
    void load_vocab(const std::string& vocab_path) {
        std::ifstream vocab_file(vocab_path);
        if (!vocab_file) throw std::runtime_error("unable to open MiniLM vocab " + vocab_path);
        std::string line;
        std::int64_t token_id = 0;
        while (std::getline(vocab_file, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            vocab_.emplace(line, token_id);
            ++token_id;
        }
        pad_id_ = require_token_id("[PAD]");
        unk_id_ = require_token_id("[UNK]");
        cls_id_ = require_token_id("[CLS]");
        sep_id_ = require_token_id("[SEP]");
    }

    void load_input_names() {
        std::size_t input_count = 0;
        ort_check(g_ort->SessionGetInputCount(session_.get(), &input_count), "SessionGetInputCount");
        if (input_count < 2 || input_count > 3)
            throw std::runtime_error("MiniLM model must take 2 or 3 inputs");
        std::vector<std::string> names;
        for (std::size_t i = 0; i < input_count; ++i) {
            char* name = nullptr;
            ort_check(g_ort->SessionGetInputName(session_.get(), i, allocator_, &name),
                      "SessionGetInputName");
            names.emplace_back(name);
            ort_check(g_ort->AllocatorFree(allocator_, name), "AllocatorFree");
        }
        // Inputs are fed in the fixed order input_ids, attention_mask[, token_type_ids].
        static const char* const expected[3] = {"input_ids", "attention_mask", "token_type_ids"};
        for (std::size_t slot = 0; slot < input_count; ++slot) {
            if (std::find(names.begin(), names.end(), expected[slot]) == names.end())
                throw std::runtime_error(std::string("missing MiniLM input: ") + expected[slot]);
            input_names_.emplace_back(expected[slot]);
        }
        for (const std::string& n : input_names_) input_name_ptrs_.push_back(n.c_str());
    }

    void load_output() {
        std::size_t output_count = 0;
        ort_check(g_ort->SessionGetOutputCount(session_.get(), &output_count), "SessionGetOutputCount");
        if (output_count == 0) throw std::runtime_error("MiniLM model has no outputs");
        char* name = nullptr;
        ort_check(g_ort->SessionGetOutputName(session_.get(), 0, allocator_, &name),
                  "SessionGetOutputName");
        output_name_ = name;
        ort_check(g_ort->AllocatorFree(allocator_, name), "AllocatorFree");

        // The width is the model's, read from its declared output shape. No default: a
        // model whose hidden size is not static cannot declare a fixed embedding width.
        OrtTypeInfo* type_raw = nullptr;
        ort_check(g_ort->SessionGetOutputTypeInfo(session_.get(), 0, &type_raw),
                  "SessionGetOutputTypeInfo");
        TypePtr type_info(type_raw);
        const OrtTensorTypeAndShapeInfo* tensor_info = nullptr;
        ort_check(g_ort->CastTypeInfoToTensorInfo(type_info.get(), &tensor_info),
                  "CastTypeInfoToTensorInfo");
        if (tensor_info == nullptr) throw std::runtime_error("MiniLM output is not a tensor");
        const std::vector<int64_t> dims = tensor_shape(tensor_info);
        if (dims.size() != 3 || dims[2] <= 0)
            throw std::runtime_error("MiniLM output must be [batch, sequence, hidden] with a "
                                     "static hidden size");
        hidden_size_ = static_cast<std::size_t>(dims[2]);
    }

    std::int64_t require_token_id(const char* token) const {
        auto found = vocab_.find(token);
        if (found == vocab_.end())
            throw std::runtime_error(std::string("missing required token in vocab: ") + token);
        return found->second;
    }

    std::vector<std::int64_t> encode(const std::string& text) const {
        std::vector<std::int64_t> token_ids;
        token_ids.reserve(max_length_);
        token_ids.push_back(cls_id_);
        const std::size_t max_pieces = max_length_ - 1;
        for (const std::string& token : basic_tokenize(normalize_text(text))) {
            for (std::int64_t piece : wordpiece(token)) {
                if (token_ids.size() >= max_pieces) break;
                token_ids.push_back(piece);
            }
            if (token_ids.size() >= max_pieces) break;
        }
        token_ids.push_back(sep_id_);
        return token_ids;
    }

    std::vector<std::int64_t> wordpiece(const std::string& token) const {
        if (token.empty()) return {};
        if (token.size() > 100) return {unk_id_};
        auto direct = vocab_.find(token);
        if (direct != vocab_.end()) return {direct->second};

        std::vector<std::int64_t> pieces;
        std::size_t start = 0;
        while (start < token.size()) {
            std::int64_t best_id = -1;
            std::size_t best_end = start;
            for (std::size_t end = token.size(); end > start; --end) {
                const std::string candidate = start == 0
                    ? token.substr(start, end - start)
                    : "##" + token.substr(start, end - start);
                auto found = vocab_.find(candidate);
                if (found != vocab_.end()) { best_id = found->second; best_end = end; break; }
            }
            if (best_id < 0) return {unk_id_};
            pieces.push_back(best_id);
            start = best_end;
        }
        return pieces;
    }

    EnvPtr env_;
    SessPtr session_;
    MemPtr memory_info_;
    OrtAllocator* allocator_ = nullptr;   // default allocator: owned by ORT, never released
    std::unordered_map<std::string, std::int64_t> vocab_;
    std::size_t max_length_;
    std::size_t hidden_size_ = 0;
    std::int64_t pad_id_ = 0;
    std::int64_t unk_id_ = 0;
    std::int64_t cls_id_ = 0;
    std::int64_t sep_id_ = 0;
    std::vector<std::string> input_names_;
    std::vector<const char*> input_name_ptrs_;
    std::string output_name_;
};

// ---------------------------------------------------------------------------
// Embedding capability kernel — draken_embed_minilm
// ---------------------------------------------------------------------------
// When registered (opteryx/types/vectors/embedding_capability.py) it replaces the core
// static-hash draken_embed, so the text cosine kernels and the vector index embed with
// MiniLM. The embedder is a process-lifetime singleton: the kernel is a bare C function
// pointer with nowhere to hold a session, and a session is thread-safe to share.
std::unique_ptr<MiniLMEmbedder> g_capability_embedder;
std::size_t g_capability_dims = 0;
std::string g_capability_model;

// Error VecResult. NOT draken_error_sentinel: that writes a thread_local buffer owned by
// whichever copy of error_handling.cpp is linked into the caller, and this kernel lives
// in a different extension. A static literal outlives every reader.
inline VecResult minilm_kernel_error(const char* msg) {
    VecResult r{};
    r.data = nullptr;
    r.error_msg = msg;
    return r;
}

}  // namespace

extern "C" VecResult draken_embed_minilm(void* ctx, const DrakenVector* const* args,
                                         uint32_t nargs) {
    if (nargs != 1u) return minilm_kernel_error("draken_embed: expected 1 argument");
    if (g_capability_embedder == nullptr)
        return minilm_kernel_error("draken_embed: minilm capability is not installed");

    const DrakenVector* v = args[0];
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
        return minilm_kernel_error("draken_embed: string operand required");

    // A model's width is not negotiable — reject a width other than the model's rather
    // than return a differently-shaped vector than the caller sized for.
    if (ctx == nullptr)
        return minilm_kernel_error("draken_embed: missing vector dimension context");
    const uint32_t dims = static_cast<const struct vector_dim_ctx*>(ctx)->dimension;
    if (dims != static_cast<uint32_t>(g_capability_dims))
        return minilm_kernel_error(
            "draken_embed: caller asked for a width this minilm capability cannot produce");

    const uint32_t n = v->length;
    const uint32_t k = v->data_length;
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);

    try {
        // Embed the K PHYSICAL values, then gather through selection — the uniform
        // data[selection[i]] read. A constant operand embeds ONCE rather than n times.
        std::vector<std::string> texts;
        texts.reserve(k);
        for (uint32_t j = 0; j < k; ++j) {
            const DrakenStringSlot* slot = &sa->slots[j];
            texts.emplace_back(reinterpret_cast<const char*>(str_data(slot, sa->arena)),
                               str_length(slot));
        }
        const std::vector<std::vector<float>> rows = g_capability_embedder->embed_texts(texts);
        if (rows.size() != texts.size())
            return minilm_kernel_error("draken_embed: minilm returned the wrong batch size");

        const size_t row_cells = static_cast<size_t>(dims);
        uint16_t* phys = static_cast<uint16_t*>(
            draken_malloc((k > 0u ? k : 1u) * row_cells * sizeof(uint16_t)));
        if (!phys) return minilm_kernel_error("draken_embed: allocation failed");
        for (uint32_t j = 0; j < k; ++j) {
            uint16_t* dst = phys + static_cast<size_t>(j) * row_cells;
            for (size_t d = 0; d < row_cells; ++d)
                dst[d] = fp16_ieee_from_fp32_value(rows[j][d]);
        }

        const size_t row_bytes = row_cells * sizeof(uint16_t);
        uint16_t* data = static_cast<uint16_t*>(draken_malloc((n > 0u ? n : 1u) * row_bytes));
        if (!data) { draken_free(phys); return minilm_kernel_error("draken_embed: allocation failed"); }

        uint8_t* validity = nullptr;
        if (v->validity != nullptr) {
            const uint32_t bm = (n + 7u) >> 3;
            const uint32_t padded = (bm + 7u) & ~7u;
            validity = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
            if (!validity) {
                draken_free(phys); draken_free(data);
                return minilm_kernel_error("draken_embed: allocation failed");
            }
            std::memcpy(validity, v->validity, bm);
            if (n & 7u) validity[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
            for (uint32_t b = bm; b < padded; ++b) validity[b] = 0u;
        }

        for (uint32_t i = 0; i < n; ++i) {
            const bool valid = (v->validity == nullptr) || ((v->validity[i >> 3] >> (i & 7u)) & 1u);
            // Null in -> null out; the row is zeroed rather than left uninitialised.
            if (!valid) std::memset(data + static_cast<size_t>(i) * row_cells, 0, row_bytes);
            else        std::memcpy(data + static_cast<size_t>(i) * row_cells,
                                    phys + static_cast<size_t>(v->selection[i]) * row_cells,
                                    row_bytes);
        }
        draken_free(phys);

        VecResult r{};
        r.data           = data;
        r.validity       = validity;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_VECTOR_FP16;
        r.flags          = DRAKEN_SEL_IDENTITY;
        r.vec_dimension  = static_cast<uint16_t>(dims);
        return r;
    } catch (const std::exception&) {
        // No Python object may be built on this path (it runs without the GIL).
        return minilm_kernel_error("draken_embed: minilm inference failed");
    }
}

NB_MODULE(minilm_native, m) {
    // Load onnxruntime from `library_path`, build the process-lifetime embedder, and hand
    // back (kernel_ptr, dimensions) for
    // opteryx.types.vectors.embedding_capability.register_embedding_capability. Returning
    // the address rather than self-registering keeps the policy (what EMBED means) in
    // Python, at registration time, instead of in an import side effect.
    m.def(
        "install_embed_capability",
        [](const std::string& library_path, const std::string& model_path,
           const std::string& vocab_path, std::size_t max_length) {
            load_onnxruntime(library_path);
            if (g_capability_embedder == nullptr) {
                g_capability_embedder =
                    std::make_unique<MiniLMEmbedder>(model_path, vocab_path, max_length);
                g_capability_dims = g_capability_embedder->dimensions();
                g_capability_model = model_path;
            } else if (model_path != g_capability_model) {
                // The kernel is process-lifetime; a second model would silently be ignored.
                throw std::runtime_error("the embedding capability is already installed from " +
                                         g_capability_model + "; one model per process");
            }
            return nb::make_tuple(reinterpret_cast<std::uintptr_t>(&draken_embed_minilm),
                                  g_capability_dims);
        },
        nb::arg("library_path"), nb::arg("model_path"), nb::arg("vocab_path"),
        nb::arg("max_length") = 256);
}
