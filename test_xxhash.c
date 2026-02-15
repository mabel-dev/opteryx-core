#define XXH_INLINE_ALL
#define XXH_NO_XXH128 1
#include "third_party/cyan4973/xxhash.h"
#include <stdio.h>
#include <string.h>

// Add a debug wrapper
unsigned long long test_hash(const char* input, size_t len) {
    printf("  Calling XXH3_64bits with len=%zu\n", len);
    unsigned long long result = XXH3_64bits(input, len);
    printf("  Result: %016llx\n", result);
    return result;
}

int main() {
    const char* tests[] = {"", "a", "ab", "abc", "abcd", "hello", "world"};
    const unsigned long long expected[] = {
        0x2d06800538d394c2ULL,
        0xd24ec4f1a98c6e5bULL,
        0x65f708ca92d04a61ULL,
        0x1b0c43f15fe7dc50ULL,
        0xf4d8a4a9f691b1a6ULL,
        0x16e527a0d7c5f20cULL,
        0x4a1ef8593a8377d6ULL
    };
    
    for (int i = 0; i < 7; i++) {
        size_t len = strlen(tests[i]);
        printf("\nTest %d: input='%s' len=%zu\n", i, tests[i], len);
        unsigned long long result = test_hash(tests[i], len);
        char status = (result == expected[i]) ? '+' : 'X';
        printf("%c expected=%016llx  got=%016llx\n", 
               status, expected[i], result);
    }
    
    return 0;
}
