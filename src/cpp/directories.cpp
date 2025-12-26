#include "directories.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <cstring>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

// List files in a directory (non-recursive)
int list_directory(const char* path, file_info_t** files, size_t* count) {
    DIR* dir = opendir(path);
    if (!dir) return -errno;
    
    struct dirent* entry;
    size_t capacity = 32;
    size_t num_files = 0;
    
    *files = (file_info_t*)malloc(capacity * sizeof(file_info_t));
    if (!*files) {
        closedir(dir);
        return -ENOMEM;
    }
    
    while ((entry = readdir(dir)) != NULL) {
        // Skip . and ..
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        
        // Resize if needed
        if (num_files >= capacity) {
            capacity *= 2;
            file_info_t* new_files = (file_info_t*)realloc(*files, capacity * sizeof(file_info_t));
            if (!new_files) {
                // Cleanup on failure
                for (size_t i = 0; i < num_files; i++) {
                    free((*files)[i].name);
                }
                free(*files);
                closedir(dir);
                return -ENOMEM;
            }
            *files = new_files;
        }
        
        // Build full path for stat
        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
        
        // Get file info
        struct stat st;
        if (stat(full_path, &st) != 0) {
            continue; // Skip files we can't stat
        }
        
        // Fill file info
        (*files)[num_files].name = strdup(entry->d_name);
        (*files)[num_files].is_directory = S_ISDIR(st.st_mode);
        (*files)[num_files].is_regular_file = S_ISREG(st.st_mode);
        (*files)[num_files].size = (int64_t)st.st_size;
        (*files)[num_files].mtime = (int64_t)st.st_mtime;
        
        num_files++;
    }
    
    closedir(dir);
    *count = num_files;
    return 0;
}

// Free file list
void free_file_list(file_info_t* files, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(files[i].name);
    }
    free(files);
}

// Recursive directory walk with callback
typedef int (*file_callback_t)(const char* path, const struct stat* st, void* user_data);

int walk_directory(const char* base_path, file_callback_t callback, void* user_data, int max_depth) {
    DIR* dir = opendir(base_path);
    if (!dir) return -errno;
    
    struct dirent* entry;
    char path[PATH_MAX];
    
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        
        snprintf(path, sizeof(path), "%s/%s", base_path, entry->d_name);
        
        struct stat st;
        if (stat(path, &st) != 0) continue;
        
        // Call callback for this entry
        int result = callback(path, &st, user_data);
        if (result != 0) {
            closedir(dir);
            return result; // Early termination if callback returns non-zero
        }
        
        // Recurse into subdirectories if we haven't hit max depth
        if (S_ISDIR(st.st_mode) && max_depth != 0) {
            int result = walk_directory(path, callback, user_data, max_depth - 1);
            if (result != 0) {
                closedir(dir);
                return result;
            }
        }
    }
    
    closedir(dir);
    return 0;
}

static std::string join_paths(const std::string& base, const char* name) {
    if (base.empty()) {
        return std::string(name);
    }
    if (base == "/") {
        return std::string("/") + name;
    }
    if (base.back() == '/') {
        std::string result(base);
        result.append(name);
        return result;
    }
    std::string result(base);
    result.push_back('/');
    result.append(name);
    return result;
}

static bool matches_extension(const char* name, const std::vector<std::string>& extensions)
{
    if (extensions.empty()) {
        return true;
    }

    const char* dot = strrchr(name, '.');
    if (!dot) {
        return false;
    }
    
    size_t ext_len = strlen(dot);
    for (const auto& ext : extensions) {
        if (ext.length() == ext_len && memcmp(dot, ext.c_str(), ext_len) == 0) {
            return true;
        }
    }
    return false;
}

static int classify_entry(const std::string& path, const struct dirent* entry, bool* is_directory,
                          bool* is_file) {
    *is_directory = false;
    *is_file = false;

#if defined(DT_DIR)
    unsigned char dtype = entry->d_type;
    if (dtype == DT_DIR) {
        *is_directory = true;
        return 0;
    }
    if (dtype == DT_REG) {
        *is_file = true;
        return 0;
    }
    if (dtype != DT_LNK && dtype != DT_UNKNOWN) {
        return 0;
    }
#endif

    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return -errno;
    }

    if (S_ISDIR(st.st_mode)) {
        *is_directory = true;
    } else if (S_ISREG(st.st_mode)) {
        *is_file = true;
    }

    return 0;
}

int list_matching_files(const char* base_path, const char** extensions, size_t ext_count,
                        char*** files, size_t* count) {
    if (!base_path || !files || !count) {
        return -EINVAL;
    }

    *files = nullptr;
    *count = 0;

    std::vector<std::string> extension_list;
    extension_list.reserve(ext_count);
    for (size_t i = 0; i < ext_count; ++i) {
        if (extensions[i] != nullptr) {
            extension_list.emplace_back(extensions[i]);
        }
    }

    std::vector<std::string> stack;
    stack.emplace_back(base_path);

    std::vector<std::string> matches;
    matches.reserve(128);

    bool processed_root = false;

    while (!stack.empty()) {
        std::string current = std::move(stack.back());
        stack.pop_back();

        DIR* dir = opendir(current.c_str());
        if (!dir) {
            int err = errno;
            if (current == base_path) {
                return -err;
            }
            // Skip directories that disappear or are inaccessible during traversal
            continue;
        }

        processed_root = true;

        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            std::string full_path = join_paths(current, entry->d_name);

            bool is_directory = false;
            bool is_file = false;
            int classify_result = classify_entry(full_path, entry, &is_directory, &is_file);
            if (classify_result != 0) {
                continue;
            }

            if (is_directory) {
                stack.emplace_back(std::move(full_path));
            } else if (is_file) {
                if (matches_extension(entry->d_name, extension_list)) {
                    matches.emplace_back(std::move(full_path));
                }
            }
        }

        closedir(dir);
    }

    if (!processed_root) {
        return -ENOENT;
    }

    const size_t total = matches.size();
    if (total == 0) {
        return 0;
    }

    char** out = (char**)malloc(total * sizeof(char*));
    if (!out) {
        return -ENOMEM;
    }

    for (size_t i = 0; i < total; ++i) {
        out[i] = strdup(matches[i].c_str());
        if (!out[i]) {
            for (size_t j = 0; j < i; ++j) {
                free(out[j]);
            }
            free(out);
            return -ENOMEM;
        }
    }

    *files = out;
    *count = total;
    return 0;
}

void free_file_names(char** files, size_t count) {
    if (!files) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free(files[i]);
    }
    free(files);
}

// Recursive listing that returns file_info_t metadata with full paths in name
int list_files_with_info(const char* base_path, const char** extensions, size_t ext_count,
                         file_info_t** files, size_t* count) {
    if (!base_path || !files || !count) {
        return -EINVAL;
    }

    std::vector<std::string> extension_list;
    extension_list.reserve(ext_count);
    for (size_t i = 0; i < ext_count; ++i) {
        if (extensions[i] != nullptr) {
            // Store extensions with leading '.' for faster comparison
            if (extensions[i][0] != '.') {
                extension_list.emplace_back(std::string(".") + extensions[i]);
            } else {
                extension_list.emplace_back(extensions[i]);
            }
        }
    }

    std::vector<std::string> stack;
    stack.emplace_back(base_path);

    std::vector<file_info_t> out_list;
    out_list.reserve(512);  // Larger initial capacity

    bool processed_root = false;

    // Pre-allocate buffer for directory entry size
    long name_max = pathconf(base_path, _PC_NAME_MAX);
    if (name_max == -1) name_max = 255;
    size_t entry_size = offsetof(struct dirent, d_name) + name_max + 1;
    
    while (!stack.empty()) {
        std::string current = std::move(stack.back());
        stack.pop_back();

        DIR* dir = opendir(current.c_str());
        if (!dir) {
            int err = errno;
            if (current == base_path) {
                return -err;
            }
            continue;
        }

        processed_root = true;

        struct dirent* entry;
        // Read directory entries and store them for batch processing
        std::vector<std::pair<std::string, struct dirent*>> entries;
        
        // First pass: collect all entries
        while ((entry = readdir(dir)) != nullptr) {
            // Skip . and ..
            if (entry->d_name[0] == '.') {
                if ((entry->d_name[1] == '\0') || 
                    (entry->d_name[1] == '.' && entry->d_name[2] == '\0')) {
                    continue;
                }
            }
            
            // Quick filter: if we have extensions and filename is too short
            if (!extension_list.empty()) {
                size_t name_len = strlen(entry->d_name);
                size_t min_ext_len = SIZE_MAX;
                for (const auto& ext : extension_list) {
                    if (ext.length() < min_ext_len) {
                        min_ext_len = ext.length();
                    }
                }
                if (name_len < min_ext_len) {
                    continue;
                }
            }
            
            // Make a copy of the dirent entry
            struct dirent* entry_copy = (struct dirent*)malloc(entry_size);
            if (!entry_copy) {
                continue;
            }
            memcpy(entry_copy, entry, entry_size);
            entries.emplace_back(current, entry_copy);
        }
        
        // Second pass: process collected entries
        for (auto& entry_pair : entries) {
            const std::string& current_dir = entry_pair.first;
            struct dirent* entry_copy = entry_pair.second;
            
            // Build full path more efficiently
            std::string full_path;
            full_path.reserve(current_dir.length() + 1 + strlen(entry_copy->d_name));
            full_path.append(current_dir);
            if (current_dir.back() != '/') {
                full_path.push_back('/');
            }
            full_path.append(entry_copy->d_name);
            
            bool is_directory = false;
            bool is_file = false;
            
            // Try to use d_type first if available
#ifdef _DIRENT_HAVE_D_TYPE
            if (entry_copy->d_type != DT_UNKNOWN) {
                if (entry_copy->d_type == DT_DIR) {
                    is_directory = true;
                } else if (entry_copy->d_type == DT_REG) {
                    is_file = true;
                }
            }
#endif
            
            // If d_type not available or is DT_UNKNOWN/DT_LNK, use stat
            if (!is_directory && !is_file) {
                struct stat st;
                if (stat(full_path.c_str(), &st) != 0) {
                    free(entry_copy);
                    continue;
                }
                
                if (S_ISDIR(st.st_mode)) {
                    is_directory = true;
                } else if (S_ISREG(st.st_mode)) {
                    is_file = true;
                }
            }
            
            if (is_directory) {
                stack.emplace_back(std::move(full_path));
            } else if (is_file) {
                // Check if file matches extensions
                if (matches_extension(entry_copy->d_name, extension_list)) {
                    // We need stat for size and mtime even if we knew it was a file from d_type
                    struct stat st;
                    if (stat(full_path.c_str(), &st) != 0) {
                        free(entry_copy);
                        continue;
                    }
                    
                    file_info_t info;
                    info.name = strdup(full_path.c_str());
                    info.is_directory = 0;
                    info.is_regular_file = 1;
                    info.size = (int64_t)st.st_size;
                    info.mtime = (int64_t)st.st_mtime;
                    out_list.push_back(info);
                }
            }
            
            free(entry_copy);
        }
        
        entries.clear();
        closedir(dir);
    }

    if (!processed_root) {
        return -ENOENT;
    }

    const size_t total = out_list.size();
    if (total == 0) {
        *files = nullptr;
        *count = 0;
        return 0;
    }

    file_info_t* out = (file_info_t*)malloc(total * sizeof(file_info_t));
    if (!out) {
        for (auto& info : out_list) {
            free(info.name);
        }
        return -ENOMEM;
    }

    // Use memcpy for bulk copy
    memcpy(out, out_list.data(), total * sizeof(file_info_t));
    *files = out;
    *count = total;
    return 0;
}
