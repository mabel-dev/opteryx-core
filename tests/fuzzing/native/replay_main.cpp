// A `main()` that feeds files to a libFuzzer entry point.
//
// Exists because the fuzzers must be runnable where libFuzzer is not. Apple's
// Command Line Tools ship libFuzzer's headers but not its runtime
// (libclang_rt.fuzzer_osx.a), so `-fsanitize=fuzzer` does not link on a stock
// macOS dev machine — the platform this repo is developed on. Without a driver
// like this, the native fuzzers would be CI-only code that a developer could
// neither run nor debug locally.
//
// It also gives the corpus a second life: replaying every seed and every
// previously-crashing input under ASan/UBSan is a fast, deterministic
// regression test, which is a different job from searching for new inputs and
// worth having on every CI run rather than nightly.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size);

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <file> [file ...]\n", argv[0]);
        return 2;
    }

    for (int i = 1; i < argc; ++i) {
        std::FILE* handle = std::fopen(argv[i], "rb");
        if (handle == nullptr) {
            std::fprintf(stderr, "%s: cannot open\n", argv[i]);
            return 1;
        }
        std::fseek(handle, 0, SEEK_END);
        const long length = std::ftell(handle);
        if (length < 0) {
            std::fprintf(stderr, "%s: cannot size\n", argv[i]);
            std::fclose(handle);
            return 1;
        }
        std::fseek(handle, 0, SEEK_SET);

        std::vector<uint8_t> buffer(static_cast<size_t>(length));
        if (length > 0 && std::fread(buffer.data(), 1, buffer.size(), handle) != buffer.size()) {
            std::fprintf(stderr, "%s: short read\n", argv[i]);
            std::fclose(handle);
            return 1;
        }
        std::fclose(handle);

        LLVMFuzzerTestOneInput(buffer.data(), buffer.size());
        std::printf("  ok  %s (%ld bytes)\n", argv[i], length);
    }

    std::printf("replayed %d input(s)\n", argc - 1);
    return 0;
}
