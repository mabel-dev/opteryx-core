# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Merge-on-read deletes on the native parquet scan.

The handle that owns one scan's DeleteAdmission (src/cpp/engine/delete_admission.hpp)
for the run: built by the compiler from the manifest's resolved delete vectors and
handed to the native scan by address. The scan decides, per row group, at execution
start, which rows it may decode.
"""

cdef extern from "engine/delete_admission.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass DeleteAdmission:
        cppclass File:
            string path
            cppvector[uint32_t] deleted
        cppvector[File] files


cdef class DeleteAdmissionHandle:
    """Owns one scan's DeleteAdmission for the run."""

    cdef DeleteAdmission* admission

    def __cinit__(self, list files):
        """`files`: (fetch path, deleted ordinals ascending) for every delete-bearing
        file of the scan."""
        cdef size_t i = 0
        cdef uint32_t ordinal
        cdef tuple entry
        self.admission = new DeleteAdmission()
        self.admission.files.resize(len(files))
        for entry in files:
            self.admission.files[i].path = (<str>entry[0]).encode("utf-8")
            for ordinal in entry[1]:
                self.admission.files[i].deleted.push_back(ordinal)
            i += 1

    def __dealloc__(self):
        if self.admission != NULL:
            del self.admission
            self.admission = NULL

    def address(self) -> int:
        return <size_t><void*>self.admission
