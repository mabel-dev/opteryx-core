#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "directories.h"
#include "disk_io.h"

#if defined(_WIN32)
#include <sys/stat.h>
#else
#include <sys/stat.h>
#endif

namespace nb = nanobind;

namespace {

struct FileListGuard {
    file_info_t* files = nullptr;
    size_t count = 0;

    ~FileListGuard() {
        if (files != nullptr) {
            free_file_list(files, count);
        }
    }
};

struct FileNamesGuard {
    char** files = nullptr;
    size_t count = 0;

    ~FileNamesGuard() {
        if (files != nullptr) {
            free_file_names(files, count);
        }
    }
};

struct MmapEntry {
    unsigned char* ptr = nullptr;
    size_t size = 0;
};

std::mutex g_mmap_mutex;
std::unordered_map<PyObject*, MmapEntry> g_mmaps;

inline int normalize_error_code(int rc) {
    int err = (rc < 0) ? -rc : rc;
    return err > 0 ? err : EIO;
}

[[noreturn]] void raise_path_error(int rc, const char* path, const char* message) {
    int err = normalize_error_code(rc);
    errno = err;

    if (err == ENOENT) {
        PyErr_SetFromErrnoWithFilename(PyExc_FileNotFoundError, path);
    } else {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
        if (message != nullptr && PyErr_Occurred()) {
            PyObject* exc = PyErr_Occurred();
            if (exc == nullptr) {
                PyErr_SetString(PyExc_OSError, message);
            }
        }
    }

    throw nb::python_error();
}

bool stat_file_size(const char* path, size_t& out_size) {
    struct stat st {};
    if (stat(path, &st) != 0) {
        return false;
    }
    if (st.st_size < 0) {
        out_size = 0;
    } else {
        out_size = static_cast<size_t>(st.st_size);
    }
    return true;
}

nb::object make_memoryview_from_object(PyObject* obj) {
    PyObject* mv = PyMemoryView_FromObject(obj);
    if (mv == nullptr) {
        throw nb::python_error();
    }
    return nb::steal<nb::object>(mv);
}

nb::object make_empty_memoryview() {
    nb::bytearray empty(nullptr, 0);
    return make_memoryview_from_object(empty.ptr());
}

nb::object read_file_impl(nb::str path, bool sequential, bool willneed, bool drop_after) {
    const char* c_path = path.c_str();
    size_t size = 0;

    if (!stat_file_size(c_path, size)) {
        raise_path_error(-errno, c_path, "Failed to stat file");
    }

    if (size == 0) {
        return make_empty_memoryview();
    }

    nb::bytearray buffer(nullptr, size);
    auto* dst = reinterpret_cast<unsigned char*>(buffer.data());
    size_t out_len = 0;

    int rc = read_all_pread(c_path, dst, &out_len, sequential, willneed, drop_after);
    if (rc != 0) {
        raise_path_error(rc, c_path, "Failed to read file");
    }

    if (out_len < size) {
        buffer.resize(out_len);
    }

    return make_memoryview_from_object(buffer.ptr());
}

void parse_extensions(nb::handle extensions_obj, std::vector<std::string>& storage) {
    if (extensions_obj.is_none()) {
        throw nb::value_error("extensions must be provided");
    }

    nb::tuple ext_seq = nb::tuple(extensions_obj);
    storage.clear();
    storage.reserve(ext_seq.size());

    for (nb::handle item : ext_seq) {
        if (nb::isinstance<nb::str>(item)) {
            storage.emplace_back(std::string(nb::str(item).c_str()));
        } else if (PyBytes_Check(item.ptr())) {
            Py_ssize_t n = PyBytes_Size(item.ptr());
            const char* ptr = PyBytes_AsString(item.ptr());
            if (ptr == nullptr) {
                throw nb::python_error();
            }
            storage.emplace_back(ptr, static_cast<size_t>(n));
        } else {
            throw nb::type_error("extensions must be a sequence of str or bytes");
        }
    }
}

}  // namespace

NB_MODULE(disk_reader, m) {
    m.def(
        "read_file",
        [](nb::str path, bool sequential, bool willneed, bool drop_after) {
            return read_file_impl(path, sequential, willneed, drop_after);
        },
        nb::arg("path"),
        nb::arg("sequential") = true,
        nb::arg("willneed") = true,
        nb::arg("drop_after") = false
    );

    m.def(
        "read_file_to_bytes",
        [](nb::str path, bool sequential, bool willneed, bool drop_after) {
            nb::object mv = read_file_impl(path, sequential, willneed, drop_after);
            return nb::bytes(nb::module_::import_("builtins").attr("bytes")(mv));
        },
        nb::arg("path"),
        nb::arg("sequential") = true,
        nb::arg("willneed") = true,
        nb::arg("drop_after") = false
    );

    m.def(
        "list_directory",
        [](nb::str path) {
            const char* c_path = path.c_str();
            FileListGuard guard {};

            int rc = list_directory(c_path, &guard.files, &guard.count);
            if (rc != 0) {
                raise_path_error(rc, c_path, "Failed to list directory");
            }

            nb::list out;
            for (size_t i = 0; i < guard.count; ++i) {
                const file_info_t& entry = guard.files[i];
                out.append(nb::make_tuple(
                    nb::str(entry.name ? entry.name : ""),
                    static_cast<bool>(entry.is_directory),
                    static_cast<bool>(entry.is_regular_file),
                    entry.size,
                    entry.mtime
                ));
            }
            return out;
        },
        nb::arg("path")
    );

    m.def(
        "list_files",
        [](nb::str path, nb::handle extensions_obj) {
            const char* c_path = path.c_str();

            std::vector<std::string> ext_storage;
            parse_extensions(extensions_obj, ext_storage);

            std::vector<const char*> ext_ptrs;
            ext_ptrs.reserve(ext_storage.size());
            for (const std::string& ext : ext_storage) {
                ext_ptrs.push_back(ext.c_str());
            }

            FileNamesGuard guard {};
            int rc = list_matching_files(
                c_path,
                ext_ptrs.empty() ? nullptr : ext_ptrs.data(),
                ext_ptrs.size(),
                &guard.files,
                &guard.count
            );
            if (rc != 0) {
                raise_path_error(rc, c_path, "Failed to list files");
            }

            nb::list out;
            for (size_t i = 0; i < guard.count; ++i) {
                out.append(nb::str(guard.files[i] ? guard.files[i] : ""));
            }
            return out;
        },
        nb::arg("path"),
        nb::arg("extensions")
    );

    m.def(
        "list_files_info",
        [](nb::str path, nb::handle extensions_obj) {
            const char* c_path = path.c_str();

            std::vector<std::string> ext_storage;
            parse_extensions(extensions_obj, ext_storage);

            std::vector<const char*> ext_ptrs;
            ext_ptrs.reserve(ext_storage.size());
            for (const std::string& ext : ext_storage) {
                ext_ptrs.push_back(ext.c_str());
            }

            FileListGuard guard {};
            int rc = list_files_with_info(
                c_path,
                ext_ptrs.empty() ? nullptr : ext_ptrs.data(),
                ext_ptrs.size(),
                &guard.files,
                &guard.count
            );
            if (rc != 0) {
                raise_path_error(rc, c_path, "Failed to list files");
            }

            nb::list out;
            for (size_t i = 0; i < guard.count; ++i) {
                const file_info_t& entry = guard.files[i];
                out.append(nb::make_tuple(
                    nb::str(entry.name ? entry.name : ""),
                    static_cast<bool>(entry.is_directory),
                    static_cast<bool>(entry.is_regular_file),
                    entry.size,
                    entry.mtime
                ));
            }
            return out;
        },
        nb::arg("path"),
        nb::arg("extensions")
    );

    m.def(
        "read_file_mmap",
        [](nb::str path) {
            const char* c_path = path.c_str();
            unsigned char* mapped_data = nullptr;
            size_t size = 0;

            int rc = read_all_mmap(c_path, &mapped_data, &size);
            if (rc != 0) {
                raise_path_error(rc, c_path, "Failed to mmap file");
            }

            if (mapped_data == nullptr || size == 0) {
                return make_empty_memoryview();
            }

            PyObject* mv = PyMemoryView_FromMemory(
                reinterpret_cast<char*>(mapped_data),
                static_cast<Py_ssize_t>(size),
                PyBUF_READ
            );
            if (mv == nullptr) {
                int unmap_rc = unmap_memory_c(mapped_data, size);
                (void) unmap_rc;
                throw nb::python_error();
            }

            {
                std::lock_guard<std::mutex> lock(g_mmap_mutex);
                Py_INCREF(mv);
                g_mmaps.emplace(mv, MmapEntry{mapped_data, size});
            }

            return nb::steal<nb::object>(mv);
        },
        nb::arg("path")
    );

    m.def(
        "unmap_memory",
        [](nb::handle mem_obj) {
            if (mem_obj.is_none()) {
                return true;
            }

            PyObject* key = mem_obj.ptr();
            MmapEntry entry {};
            bool found = false;

            {
                std::lock_guard<std::mutex> lock(g_mmap_mutex);
                auto it = g_mmaps.find(key);
                if (it != g_mmaps.end()) {
                    entry = it->second;
                    Py_DECREF(it->first);
                    g_mmaps.erase(it);
                    found = true;
                } else if (PyMemoryView_Check(key)) {
                    PyObject* base = PyMemoryView_GET_BASE(key);
                    if (base != nullptr) {
                        auto base_it = g_mmaps.find(base);
                        if (base_it != g_mmaps.end()) {
                            entry = base_it->second;
                            Py_DECREF(base_it->first);
                            g_mmaps.erase(base_it);
                            found = true;
                        }
                    }
                }
            }

            if (!found) {
                return true;
            }

            if (entry.ptr == nullptr || entry.size == 0) {
                return true;
            }

            return unmap_memory_c(entry.ptr, entry.size) == 0;
        },
        nb::arg("mem_obj")
    );
}
