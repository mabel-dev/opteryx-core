#ifndef OPTERYX_DIRECTORIES_H
#define OPTERYX_DIRECTORIES_H

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char* name;
    int is_directory;
    int is_regular_file;
    int64_t size;
    int64_t mtime_ns;   // modification time, nanoseconds since the epoch
} file_info_t;

// st_mtime at full (nanosecond) resolution — seconds alone let a same-size
// rewrite inside one second look unchanged to anything keyed on it.
int64_t stat_mtime_ns(const struct stat* st);

int list_directory(const char* path, file_info_t** files, size_t* count);
void free_file_list(file_info_t* files, size_t count);


// Recursive listing that returns file metadata (full path in name)
int list_files_with_info(const char* base_path, const char** extensions, size_t ext_count,
                         file_info_t** files, size_t* count);


#ifdef __cplusplus
}
#endif

#endif
