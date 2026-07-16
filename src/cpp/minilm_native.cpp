#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <onnxruntime_cxx_api.h>

// Draken vector ABI — this file produces a DrakenVector result for the EMBED capability
// kernel below. The symbols (draken_malloc / draken_identity_sel) live in draken's
// extension and resolve at load time, the same way opteryx.compiled.vector_ops'
// draken__dfa_extract kernel reaches them.
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/fp16.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "ops/kernels/kernel_context.h"
#include "ops/vec_result.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdint>
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

Ort::Env& ort_env() {
    static Ort::Env env(ORT_LOGGING_LEVEL_WARNING, "opteryx_minilm");
    return env;
}

bool is_whitespace(unsigned char ch) {
    return std::isspace(ch) != 0;
}

bool is_control(unsigned char ch) {
    return ch < 32 && ch != '\t' && ch != '\n' && ch != '\r';
}

bool is_punctuation(unsigned char ch) {
    return std::ispunct(ch) != 0;
}

std::string normalize_text(std::string_view text) {
    std::string out;
    out.reserve(text.size());
    for (unsigned char ch : text) {
        if (is_control(ch)) {
            continue;
        }
        if (is_whitespace(ch)) {
            out.push_back(' ');
            continue;
        }
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
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            continue;
        }

        if (is_punctuation(ch)) {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            tokens.emplace_back(1, static_cast<char>(ch));
            continue;
        }

        current.push_back(static_cast<char>(ch));
    }

    if (!current.empty()) {
        tokens.push_back(current);
    }

    return tokens;
}

class MiniLMEmbedder {
  public:
    MiniLMEmbedder(std::string model_path, std::string vocab_path, std::size_t max_length = 256)
        : allocator_(), max_length_(max_length) {
        if (max_length_ < 3) {
            throw nb::value_error("max_length must be at least 3");
        }

        load_vocab(vocab_path);

        Ort::SessionOptions session_options;
        session_options.SetIntraOpNumThreads(1);
        session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);
        session_ = std::make_unique<Ort::Session>(ort_env(), model_path.c_str(), session_options);

        load_input_names();
        load_output_name();
    }

    std::vector<float> embed_text(std::string const& text) const {
        auto batch = embed_texts(std::vector<std::string>{text});
        return batch.empty() ? std::vector<float>{} : std::move(batch.front());
    }

    std::vector<std::vector<float>> embed_texts(std::vector<std::string> const& texts) const {
        if (texts.empty()) {
            return {};
        }

        const std::size_t batch_size = texts.size();
        std::vector<std::vector<std::int64_t>> encoded_rows;
        encoded_rows.reserve(batch_size);
        std::size_t sequence_length = 0;

        for (std::size_t row = 0; row < batch_size; ++row) {
            auto encoded = encode(texts[row]);
            sequence_length = std::max(sequence_length, encoded.size());
            encoded_rows.push_back(std::move(encoded));
        }

        if (sequence_length == 0) {
            sequence_length = 2;
        }

        std::vector<std::int64_t> input_ids(batch_size * sequence_length, pad_id_);
        std::vector<std::int64_t> attention_mask(batch_size * sequence_length, 0);
        std::vector<std::int64_t> token_type_ids(batch_size * sequence_length, 0);

        for (std::size_t row = 0; row < batch_size; ++row) {
            auto const& encoded = encoded_rows[row];
            for (std::size_t col = 0; col < encoded.size(); ++col) {
                input_ids[row * sequence_length + col] = encoded[col];
                attention_mask[row * sequence_length + col] = 1;
            }
        }

        std::array<std::int64_t, 2> input_shape {
            static_cast<std::int64_t>(batch_size),
            static_cast<std::int64_t>(sequence_length),
        };

        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
        Ort::Value input_ids_tensor = Ort::Value::CreateTensor<std::int64_t>(
            memory_info, input_ids.data(), input_ids.size(), input_shape.data(), input_shape.size()
        );
        Ort::Value attention_mask_tensor = Ort::Value::CreateTensor<std::int64_t>(
            memory_info, attention_mask.data(), attention_mask.size(), input_shape.data(), input_shape.size()
        );
        Ort::Value token_type_ids_tensor = Ort::Value::CreateTensor<std::int64_t>(
            memory_info, token_type_ids.data(), token_type_ids.size(), input_shape.data(), input_shape.size()
        );

        std::array<Ort::Value, 3> input_tensors {
            std::move(input_ids_tensor),
            std::move(attention_mask_tensor),
            std::move(token_type_ids_tensor),
        };

        auto output_tensors = session_->Run(
            Ort::RunOptions{nullptr},
            input_name_ptrs_.data(),
            input_tensors.data(),
            input_tensors.size(),
            output_name_ptrs_.data(),
            output_name_ptrs_.size()
        );

        if (output_tensors.empty() || !output_tensors[0].IsTensor()) {
            throw std::runtime_error("MiniLM inference did not return a tensor output");
        }

        Ort::Value& output = output_tensors[0];
        auto output_info = output.GetTensorTypeAndShapeInfo();
        auto output_shape = output_info.GetShape();
        if (output_shape.size() != 3) {
            throw std::runtime_error("MiniLM output tensor has unexpected rank");
        }

        const std::size_t output_batch = static_cast<std::size_t>(output_shape[0]);
        const std::size_t output_seq = static_cast<std::size_t>(output_shape[1]);
        const std::size_t hidden_size = static_cast<std::size_t>(output_shape[2]);
        if (output_batch != batch_size || output_seq != sequence_length) {
            throw std::runtime_error("MiniLM output tensor shape does not match input shape");
        }

        const float* output_data = output.GetTensorData<float>();
        std::vector<std::vector<float>> embeddings(batch_size, std::vector<float>(hidden_size, 0.0f));

        for (std::size_t row = 0; row < batch_size; ++row) {
            float token_count = 0.0f;
            for (std::size_t col = 0; col < sequence_length; ++col) {
                if (attention_mask[row * sequence_length + col] == 0) {
                    continue;
                }

                const std::size_t offset = (row * sequence_length + col) * hidden_size;
                for (std::size_t dim = 0; dim < hidden_size; ++dim) {
                    embeddings[row][dim] += output_data[offset + dim];
                }
                token_count += 1.0f;
            }

            if (token_count == 0.0f) {
                continue;
            }

            float norm = 0.0f;
            for (float& value : embeddings[row]) {
                value /= token_count;
                norm += value * value;
            }

            norm = std::sqrt(norm);
            if (norm > 0.0f) {
                for (float& value : embeddings[row]) {
                    value /= norm;
                }
            }
        }

        return embeddings;
    }

    nb::tuple score_string_vector(
        std::string const& query,
        nb::object data_buffer_obj,
        nb::object offsets_buffer_obj,
        nb::object null_buffer_obj,
        std::size_t row_count
    ) const {
        struct BufferGuard {
            Py_buffer view{};
            bool acquired = false;

            ~BufferGuard() {
                if (acquired) {
                    PyBuffer_Release(&view);
                }
            }
        };

        BufferGuard data_guard;
        BufferGuard offsets_guard;
        BufferGuard null_guard;

        if (PyObject_GetBuffer(data_buffer_obj.ptr(), &data_guard.view, PyBUF_SIMPLE) != 0) {
            throw nb::python_error();
        }
        data_guard.acquired = true;

        if (PyObject_GetBuffer(offsets_buffer_obj.ptr(), &offsets_guard.view, PyBUF_SIMPLE) != 0) {
            throw nb::python_error();
        }
        offsets_guard.acquired = true;

        if (PyObject_GetBuffer(null_buffer_obj.ptr(), &null_guard.view, PyBUF_SIMPLE) != 0) {
            throw nb::python_error();
        }
        null_guard.acquired = true;

        if (offsets_guard.view.len < static_cast<Py_ssize_t>((row_count + 1) * sizeof(std::int32_t))) {
            throw nb::value_error("StringVector offsets buffer is shorter than expected");
        }

        const char* data = static_cast<const char*>(data_guard.view.buf);
        const auto* offsets = static_cast<const std::int32_t*>(offsets_guard.view.buf);
        const auto* nulls =
            null_guard.view.len >= static_cast<Py_ssize_t>((row_count + 7) >> 3)
                ? static_cast<const std::uint8_t*>(null_guard.view.buf)
                : nullptr;

        std::vector<std::int64_t> positions;
        std::vector<std::string> texts;
        positions.reserve(row_count);
        texts.reserve(row_count);

        for (std::size_t row = 0; row < row_count; ++row) {
            if (nulls != nullptr && ((nulls[row >> 3] >> (row & 7)) & 1U) == 0U) {
                continue;
            }

            const auto start = offsets[row];
            const auto end = offsets[row + 1];
            if (end < start) {
                throw nb::value_error("StringVector offsets are not monotonic");
            }

            const char* value_ptr = data + start;
            std::size_t value_len = static_cast<std::size_t>(end - start);

            while (value_len > 0 && is_whitespace(static_cast<unsigned char>(*value_ptr))) {
                ++value_ptr;
                --value_len;
            }
            while (
                value_len > 0
                && is_whitespace(static_cast<unsigned char>(value_ptr[value_len - 1]))
            ) {
                --value_len;
            }
            if (value_len == 0) {
                continue;
            }

            positions.push_back(static_cast<std::int64_t>(row));
            texts.emplace_back(value_ptr, value_len);
        }

        if (texts.empty()) {
            return nb::make_tuple(std::move(positions), std::vector<float>{});
        }

        std::vector<std::string> batch;
        batch.reserve(texts.size() + 1);
        batch.push_back(query);
        batch.insert(batch.end(), texts.begin(), texts.end());

        auto embeddings = embed_texts(batch);
        auto const& query_embedding = embeddings.front();
        std::vector<float> scores;
        scores.reserve(texts.size());

        for (std::size_t row = 1; row < embeddings.size(); ++row) {
            auto const& embedding = embeddings[row];
            float score = 0.0f;
            for (std::size_t dim = 0; dim < query_embedding.size(); ++dim) {
                score += query_embedding[dim] * embedding[dim];
            }
            scores.push_back(score);
        }

        return nb::make_tuple(std::move(positions), std::move(scores));
    }

    std::size_t dimensions() const {
        return hidden_size_hint_;
    }

  private:
    void load_vocab(std::string const& vocab_path) {
        std::ifstream vocab_file(vocab_path);
        if (!vocab_file) {
            throw std::runtime_error("Unable to open MiniLM vocab.txt");
        }

        std::string line;
        std::int64_t token_id = 0;
        while (std::getline(vocab_file, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            vocab_.emplace(line, token_id);
            ++token_id;
        }

        pad_id_ = require_token_id("[PAD]");
        unk_id_ = require_token_id("[UNK]");
        cls_id_ = require_token_id("[CLS]");
        sep_id_ = require_token_id("[SEP]");
    }

    void load_input_names() {
        const std::size_t input_count = session_->GetInputCount();
        input_names_.resize(input_count);
        input_name_ptrs_.resize(input_count);

        for (std::size_t index = 0; index < input_count; ++index) {
            auto name = session_->GetInputNameAllocated(index, allocator_);
            input_names_[index] = name.get();
        }

        auto set_name = [&](std::size_t slot, char const* expected) {
            auto found = std::find(input_names_.begin(), input_names_.end(), expected);
            if (found == input_names_.end()) {
                throw std::runtime_error(std::string("Missing MiniLM input: ") + expected);
            }
            input_name_ptrs_[slot] = found->c_str();
        };

        if (input_count < 2) {
            throw std::runtime_error("MiniLM model has too few inputs");
        }

        set_name(0, "input_ids");
        set_name(1, "attention_mask");
        if (input_count >= 3) {
            set_name(2, "token_type_ids");
            input_name_ptrs_.resize(3);
        } else {
            input_name_ptrs_.resize(2);
        }
    }

    void load_output_name() {
        if (session_->GetOutputCount() == 0) {
            throw std::runtime_error("MiniLM model has no outputs");
        }
        auto output_name = session_->GetOutputNameAllocated(0, allocator_);
        output_name_ = output_name.get();
        output_name_ptrs_.push_back(output_name_.c_str());

        try {
            auto output_info = session_->GetOutputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();
            if (output_info.size() == 3 && output_info[2] > 0) {
                hidden_size_hint_ = static_cast<std::size_t>(output_info[2]);
            }
        } catch (...) {
            hidden_size_hint_ = 384;
        }
    }

    std::int64_t require_token_id(char const* token) const {
        auto found = vocab_.find(token);
        if (found == vocab_.end()) {
            throw std::runtime_error(std::string("Missing required token in vocab: ") + token);
        }
        return found->second;
    }

    std::vector<std::int64_t> encode(std::string const& text) const {
        std::vector<std::int64_t> token_ids;
        token_ids.reserve(max_length_);
        token_ids.push_back(cls_id_);

        auto basic_tokens = basic_tokenize(normalize_text(text));
        const std::size_t max_pieces = max_length_ - 1;
        for (std::string const& token : basic_tokens) {
            auto pieces = wordpiece(token);
            for (std::int64_t piece : pieces) {
                if (token_ids.size() >= max_pieces) {
                    break;
                }
                token_ids.push_back(piece);
            }
            if (token_ids.size() >= max_pieces) {
                break;
            }
        }

        token_ids.push_back(sep_id_);
        return token_ids;
    }

    std::vector<std::int64_t> wordpiece(std::string const& token) const {
        if (token.empty()) {
            return {};
        }
        if (token.size() > 100) {
            return {unk_id_};
        }

        auto direct = vocab_.find(token);
        if (direct != vocab_.end()) {
            return {direct->second};
        }

        std::vector<std::int64_t> pieces;
        std::size_t start = 0;
        while (start < token.size()) {
            std::int64_t best_id = -1;
            std::size_t best_end = start;

            for (std::size_t end = token.size(); end > start; --end) {
                std::string candidate;
                if (start == 0) {
                    candidate = token.substr(start, end - start);
                } else {
                    candidate = "##" + token.substr(start, end - start);
                }

                auto found = vocab_.find(candidate);
                if (found != vocab_.end()) {
                    best_id = found->second;
                    best_end = end;
                    break;
                }
            }

            if (best_id < 0) {
                return {unk_id_};
            }

            pieces.push_back(best_id);
            start = best_end;
        }

        return pieces;
    }

    Ort::AllocatorWithDefaultOptions allocator_;
    std::unique_ptr<Ort::Session> session_;
    std::unordered_map<std::string, std::int64_t> vocab_;
    std::size_t max_length_;
    std::size_t hidden_size_hint_ = 384;
    std::int64_t pad_id_ = 0;
    std::int64_t unk_id_ = 100;
    std::int64_t cls_id_ = 101;
    std::int64_t sep_id_ = 102;
    std::vector<std::string> input_names_;
    std::vector<char const*> input_name_ptrs_;
    std::string output_name_;
    std::vector<char const*> output_name_ptrs_;
};


// ---------------------------------------------------------------------------
// EMBED capability kernel — draken_embed_minilm
// ---------------------------------------------------------------------------
// An INSTALLABLE capability (opteryx/types/vectors/embedding_capability.py): when
// registered it replaces the core static-hash draken_embed, so EMBED means real
// semantic embeddings instead of lexical n-gram overlap. It is not part of the
// zero-dependency core and cannot be — it needs ONNX Runtime and a 90MB model, and the
// extension only builds under OPTERYX_BUILD_EMBEDDINGS=1. That is exactly why EMBED's
// core kernel is the static hash: the engine must never fail to plan an EMBED because
// an optional dependency is absent.
//
// The embedder is a process-lifetime singleton: the kernel is a bare C function pointer
// with nowhere to hold a session, and an ORT session is expensive and thread-safe to
// share. Installed once, never replaced (the registry refuses a width change once EMBED
// has been planned).
std::unique_ptr<MiniLMEmbedder> g_capability_embedder;
std::size_t g_capability_dims = 0;

// Error VecResult. NOT draken_error_sentinel: that writes a thread_local buffer owned by
// whichever copy of error_handling.cpp is linked into the caller, and this kernel lives
// in a different extension. A static literal outlives every reader, which is strictly
// stronger than the sentinel's contract.
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

    // The binder declared EMBED's width from THIS capability's `dimensions`, so a
    // mismatch means the plan was built against a different capability. Unlike the
    // hashed projection, a model's width is not negotiable — reject rather than return
    // a differently-shaped vector than the plan's type promises.
    if (ctx == nullptr)
        return minilm_kernel_error("draken_embed: missing vector dimension context");
    const uint32_t dims = static_cast<const struct vector_dim_ctx*>(ctx)->dimension;
    if (dims != static_cast<uint32_t>(g_capability_dims))
        return minilm_kernel_error(
            "draken_embed: plan declared a width this minilm capability cannot produce");

    const uint32_t n = v->length;
    const uint32_t k = v->data_length;
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);

    try {
        // Embed the K PHYSICAL values, then gather through selection — the uniform
        // data[selection[i]] read. A constant operand embeds ONCE rather than n times,
        // which for a model this size is the difference between one inference and n.
        std::vector<std::string> texts;
        texts.reserve(k);
        for (uint32_t j = 0; j < k; ++j) {
            const DrakenStringSlot* slot = &sa->slots[j];
            texts.emplace_back(reinterpret_cast<const char*>(str_data(slot, sa->arena)),
                               str_length(slot));
        }
        // Mean-pooled and L2-normalised fp32 rows.
        const std::vector<std::vector<float>> rows = g_capability_embedder->embed_texts(texts);
        if (rows.size() != texts.size())
            return minilm_kernel_error("draken_embed: minilm returned the wrong batch size");

        const size_t row_cells = static_cast<size_t>(dims);
        uint16_t* phys = static_cast<uint16_t*>(
            draken_malloc((k > 0u ? k : 1u) * row_cells * sizeof(uint16_t)));
        if (!phys) return minilm_kernel_error("draken_embed: allocation failed");
        for (uint32_t j = 0; j < k; ++j) {
            if (rows[j].size() != row_cells) {
                draken_free(phys);
                return minilm_kernel_error("draken_embed: minilm returned an unexpected width");
            }
            uint16_t* dst = phys + static_cast<size_t>(j) * row_cells;
            for (size_t d = 0; d < row_cells; ++d)
                dst[d] = fp16_ieee_from_fp32_value(rows[j][d]);
        }

        const size_t row_bytes = row_cells * sizeof(uint16_t);
        uint16_t* data = static_cast<uint16_t*>(
            draken_malloc((n > 0u ? n : 1u) * row_bytes));
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
            const bool valid = (v->validity == nullptr)
                             || ((v->validity[i >> 3] >> (i & 7u)) & 1u);
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
        // embed_texts throws std::runtime_error on a malformed model output. Swallowing
        // the text keeps this nogil-safe (no Python object is constructed on this path).
        return minilm_kernel_error("draken_embed: minilm inference failed");
    } catch (...) {
        return minilm_kernel_error("draken_embed: minilm inference failed (unknown)");
    }
}

NB_MODULE(minilm_native, m) {
    nb::class_<MiniLMEmbedder>(m, "MiniLMEmbedder")
        .def(nb::init<std::string, std::string, std::size_t>(), nb::arg("model_path"), nb::arg("vocab_path"), nb::arg("max_length") = 256)
        .def("embed_text", &MiniLMEmbedder::embed_text, nb::arg("text"))
        .def("embed_texts", &MiniLMEmbedder::embed_texts, nb::arg("texts"))
        .def(
            "score_string_vector",
            &MiniLMEmbedder::score_string_vector,
            nb::arg("query"),
            nb::arg("data_buffer"),
            nb::arg("offsets_buffer"),
            nb::arg("null_buffer"),
            nb::arg("row_count")
        )
        .def_prop_ro("dimensions", &MiniLMEmbedder::dimensions);

    // Construct the process-lifetime embedder and hand back (kernel_ptr, dimensions) for
    // embedding_capability.register_embedding_capability(). Returning the address rather
    // than self-registering keeps the policy (what EMBED means) in Python, where the
    // width is declared, instead of hidden in a module import side effect.
    m.def(
        "install_embed_capability",
        [](std::string model_path, std::string vocab_path, std::size_t max_length) {
            if (g_capability_embedder == nullptr) {
                g_capability_embedder = std::make_unique<MiniLMEmbedder>(
                    std::move(model_path), std::move(vocab_path), max_length);
                g_capability_dims = g_capability_embedder->dimensions();
            }
            return nb::make_tuple(
                reinterpret_cast<std::uintptr_t>(&draken_embed_minilm),
                g_capability_dims);
        },
        nb::arg("model_path"), nb::arg("vocab_path"), nb::arg("max_length") = 256,
        "Install the MiniLM EMBED kernel. Returns (kernel_ptr, dimensions) to pass to "
        "opteryx.types.vectors.embedding_capability.register_embedding_capability."
    );
}
