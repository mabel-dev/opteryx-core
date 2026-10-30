#include "directories.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <vector>
#include <cstring>
#ifdef __APPLE__
#include <sys/attr.h>
#include <sys/vnode.h>
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

// List files in a directory (non-recursive)
int64_t stat_mtime_ns(const struct stat* st) {
#if defined(__APPLE__)
    return (int64_t)st->st_mtimespec.tv_sec * 1000000000LL + (int64_t)st->st_mtimespec.tv_nsec;
#else
    return (int64_t)st->st_mtim.tv_sec * 1000000000LL + (int64_t)st->st_mtim.tv_nsec;
#endif
}

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

        // lstat, NOT stat: a symlink must be classified as itself, never as
        // whatever it points at. stat() follows the link and reports the
        // TARGET's type — a symlink to a directory would report is_directory,
        // and a symlink whose target no longer exists would silently vanish
        // from the listing instead of being reported. Neither is acceptable:
        // symlinks are not traversed by this codebase (see list_files_with_info).
        struct stat st;
        if (lstat(full_path, &st) != 0) {
            continue; // Skip files we can't stat
        }
        if (S_ISLNK(st.st_mode)) {
            continue; // symlinks are invisible to directory listing, not followed
        }

        // Fill file info
        (*files)[num_files].name = strdup(entry->d_name);
        (*files)[num_files].is_directory = S_ISDIR(st.st_mode);
        (*files)[num_files].is_regular_file = S_ISREG(st.st_mode);
        (*files)[num_files].size = (int64_t)st.st_size;
        (*files)[num_files].mtime_ns = stat_mtime_ns(&st);

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

        // lstat, NOT stat: never resolve through a symlink to classify it —
        // see the comment in list_directory above.
        struct stat st;
        if (lstat(path, &st) != 0) continue;
        if (S_ISLNK(st.st_mode)) continue; // symlinks are not walked, period

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

// A symlink is classified as neither a directory nor a file — this walker does
// not traverse or read through symlinks, ever. DT_LNK is handled explicitly
// (not folded into the DT_UNKNOWN fallback) so that intent reads directly off
// this function rather than being an emergent side effect of lstat() below.
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
    if (dtype == DT_LNK) {
        return 0; // symlink: not a directory, not a file — not followed
    }
    if (dtype != DT_UNKNOWN) {
        return 0;
    }
#endif

    // dtype was DT_UNKNOWN (filesystem doesn't populate d_type) — classify via
    // lstat, NOT stat, so an unresolved d_type never silently resolves through
    // a symlink to its target's type.
    struct stat st;
    if (lstat(path.c_str(), &st) != 0) {
        return -errno;
    }

    if (S_ISDIR(st.st_mode)) {
        *is_directory = true;
    } else if (S_ISREG(st.st_mode)) {
        *is_file = true;
    }
    // S_ISLNK falls through with both flags false — symlinks are not followed.

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
// One directory's entries for list_files_with_info. Every entry is reported as
// what it IS, never what it points at: a symlink is neither a directory to
// recurse into nor a file to read (`kind` 0 = skip), so the walk never follows one.
// Size and mtime are the regular file's own (`stat` semantics — the entry is
// already known not to be a link).
//
// MEASURED 2026-10-25 (Mac M5 Pro, 100-file ClickBench dataset, per listing):
// this walk took 229 us through the old implementation — a ~1 KB dirent copy and
// a path string copy per entry, a full-path lstat AND stat per file on macOS
// (which does not define _DIRENT_HAVE_D_TYPE) — against 82 us for readdir +
// fstatat on the directory fd and 41 us for getattrlistbulk, all three giving
// identical names, sizes and mtimes. It runs on every query that binds a local
// dataset (the manifest cache's file-set signature is built from it).
namespace {
struct WalkEntry {
    const char* name;
    int kind;            // 0 skip, 1 directory, 2 regular file
    int64_t size;
    int64_t mtime_ns;
};

template <class Visit>
int read_directory_entries(const std::string& dir_path, Visit&& visit) {
#ifdef __APPLE__
    // getattrlistbulk: names, types, mtimes and sizes for many entries per call,
    // instead of readdir plus a stat per file. "." and ".." are never returned.
    const int fd = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -errno;
    struct attrlist al {};
    al.bitmapcount = ATTR_BIT_MAP_COUNT;
    al.commonattr = ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_ERROR | ATTR_CMN_NAME |
                    ATTR_CMN_OBJTYPE | ATTR_CMN_MODTIME;
    al.fileattr = ATTR_FILE_DATALENGTH;
    alignas(8) char buf[64 * 1024];
    for (;;) {
        const int n = getattrlistbulk(fd, &al, buf, sizeof buf, 0);
        if (n == 0) break;   // end of directory
        if (n < 0) {         // a failed read must not pass for the end of the listing
            const int e = errno;
            close(fd);
            return -e;
        }
        const char* p = buf;
        for (int k = 0; k < n; ++k) {
            const char* rec = p;
            uint32_t rec_len;
            memcpy(&rec_len, p, sizeof rec_len);
            p += sizeof(uint32_t);
            attribute_set_t ret;
            memcpy(&ret, p, sizeof ret);
            p += sizeof(attribute_set_t);
            uint32_t err = 0;
            if (ret.commonattr & ATTR_CMN_ERROR) {
                memcpy(&err, p, sizeof err);
                p += sizeof(uint32_t);
            }
            const char* name = nullptr;
            if (ret.commonattr & ATTR_CMN_NAME) {
                attrreference_t ref;
                memcpy(&ref, p, sizeof ref);
                name = p + ref.attr_dataoffset;
                p += sizeof(attrreference_t);
            }
            fsobj_type_t type = VNON;
            if (ret.commonattr & ATTR_CMN_OBJTYPE) {
                memcpy(&type, p, sizeof type);
                p += sizeof(fsobj_type_t);
            }
            struct timespec mt {};
            if (ret.commonattr & ATTR_CMN_MODTIME) {
                memcpy(&mt, p, sizeof mt);
                p += sizeof(struct timespec);
            }
            off_t size = 0;
            bool have_size = false;
            if (ret.fileattr & ATTR_FILE_DATALENGTH) {
                memcpy(&size, p, sizeof size);
                have_size = true;
            }
            // An entry whose attributes could not be read fails the listing (a
            // silently short listing is a wrong COUNT(*)); one that vanished
            // since the directory was read was never there to count.
            if (err != 0 && err != ENOENT) {
                close(fd);
                return -static_cast<int>(err);
            }
            if (err == 0 && name != nullptr) {
                WalkEntry e{name, 0, 0, 0};
                if (type == VDIR) {
                    e.kind = 1;
                } else if (type == VREG && have_size &&
                           (ret.commonattr & ATTR_CMN_MODTIME)) {
                    e.kind = 2;
                    e.size = static_cast<int64_t>(size);
                    e.mtime_ns = (int64_t)mt.tv_sec * 1000000000LL + (int64_t)mt.tv_nsec;
                }
                if (e.kind != 0) visit(e);
            }
            p = rec + rec_len;
        }
    }
    close(fd);
    return 0;
#else
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) return -errno;
    const int dfd = dirfd(dir);
    struct dirent* entry;
    for (;;) {
        errno = 0;
        entry = readdir(dir);
        if (entry == nullptr) {
            if (errno != 0) {   // a read error, not the end of the directory
                const int e = errno;
                closedir(dir);
                return -e;
            }
            break;
        }
        const char* name = entry->d_name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) continue;
        int kind = 0;
        bool have_stat = false;
        struct stat st;
#ifdef _DIRENT_HAVE_D_TYPE
        if (entry->d_type == DT_DIR) kind = 1;
        else if (entry->d_type == DT_REG) kind = 2;
        else if (entry->d_type == DT_LNK) continue;   // never followed, never reported
        else if (entry->d_type != DT_UNKNOWN) continue;
#endif
        if (kind == 0) {
            // Type unknown: lstat semantics, so a link is never resolved to its target.
            if (fstatat(dfd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
                if (errno == ENOENT) continue;   // removed since readdir: not a loss
                const int e = errno;
                closedir(dir);
                return -e;
            }
            if (S_ISDIR(st.st_mode)) kind = 1;
            else if (S_ISREG(st.st_mode)) { kind = 2; have_stat = true; }
            else continue;
        }
        WalkEntry e{name, kind, 0, 0};
        if (kind == 2) {
            if (!have_stat && fstatat(dfd, name, &st, 0) != 0) {
                if (errno == ENOENT) continue;   // removed since readdir: not a loss
                const int e = errno;
                closedir(dir);
                return -e;
            }
            e.size = (int64_t)st.st_size;
            e.mtime_ns = stat_mtime_ns(&st);
        }
        visit(e);
    }
    closedir(dir);
    return 0;
#endif
}
}  // namespace

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

    // Kept from the previous walk, unchanged: with an extension filter, ANY entry
    // whose name is shorter than the shortest extension is skipped before it is
    // classified — directories included, so a short-named directory is not
    // descended into. (Reported 2026-10-25; the production listing passes no
    // extensions, where this never applies.)
    size_t min_ext_len = SIZE_MAX;
    for (const auto& ext : extension_list) min_ext_len = std::min(min_ext_len, ext.length());

    std::vector<std::string> stack;
    stack.emplace_back(base_path);
    std::vector<file_info_t> out_list;
    out_list.reserve(512);
    std::string full_path;
    bool root = true;

    while (!stack.empty()) {
        std::string current = std::move(stack.back());
        stack.pop_back();
        const bool needs_sep = current.empty() || current.back() != '/';
        auto path_of = [&](const char* name) {
            full_path.assign(current);
            if (needs_sep) full_path.push_back('/');
            full_path.append(name);
        };
        int rc = read_directory_entries(current, [&](const WalkEntry& e) {
            if (!extension_list.empty() && strlen(e.name) < min_ext_len) return;
            path_of(e.name);
            if (e.kind == 1) {
                stack.push_back(full_path);
            } else if (matches_extension(e.name, extension_list)) {
                file_info_t info;
                info.name = strdup(full_path.c_str());
                info.is_directory = 0;
                info.is_regular_file = 1;
                info.size = e.size;
                info.mtime_ns = e.mtime_ns;
                out_list.push_back(info);
            }
        });
        // Any directory that cannot be read fails the listing: skipping an
        // unreadable partition returned a short dataset with no error. One that
        // vanished mid-walk (ENOENT below the root) held nothing to miss.
        if (rc != 0 && !(rc == -ENOENT && !root)) {
            for (auto& info : out_list) free(info.name);
            return rc;
        }
        root = false;
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
    memcpy(out, out_list.data(), total * sizeof(file_info_t));
    *files = out;
    *count = total;
    return 0;
}
