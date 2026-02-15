#define XXH_INLINE_ALL
#define XXH_NO_XXH128 1
#include "third_party/cyan4973/xxhash.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Correct reference values from real upstream xxhash v0.8.3
    const char* tests[] = {"", "a", "ab", "abc", "abcd", "hello", "world", "0123456789ABCDEF"};
    const unsigned long long expected[] = {
        0x2d06800538d394c2ULL,
        0xe6c632b61e964e1fULL,
        0xa873719c24d5735cULL,
        0x78af5f94892f3950ULL,
        0x6497a96f53a89890ULL,
        0x9555e8555c62dcfdULL,
        0xd6476c25083d69beULL,
        0x2bad8ba41856a3cdULL
    };
    
    int all_pass = 1;
    for (int i = 0; i < 8; i++) {
        size_t len = strlen(tests[i]);
        unsigned long long result = XXH3_64bits(tests[i], len);
        char status = (result == expected[i]) ? '+' : 'X';
        if (result != expected[i]) all_pass = 0;
        printf("%c len=%2zu input='%-16s' result=%016llx\n", 
               status, len, tests[i], result);
    }
    
    printf("\n%s\n", all_pass ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
    return all_pass ? 0 : 1;
}
