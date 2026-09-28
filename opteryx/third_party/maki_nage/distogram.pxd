# cython: language_level=3
# distutils: language = c++

# The native Distogram (_distogram.hpp) and its Python wrapper, declared here so
# native code (the manifest's histogram fold) hands one over without Python.

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector


cdef extern from "opteryx/third_party/maki_nage/_distogram.hpp" namespace "maki_nage":
    cdef cppclass DistogramBin "maki_nage::Bin":
        double value
        int64_t count

    cdef cppclass DistogramCore "maki_nage::Distogram":
        DistogramCore() except +
        DistogramCore(int64_t bin_count) except +
        @staticmethod
        DistogramCore from_counts(const int64_t* counts, int64_t num_bins, double minimum, double maximum)
        void update(double value, int64_t count) except +
        void merge(const DistogramCore& other) except +
        double count_up_to(double value)
        int64_t count()
        int64_t bin_count()
        int64_t max_bin_count()
        double min()
        double max()
        const vector[DistogramBin]& bins()


cdef class Distogram:
    cdef shared_ptr[DistogramCore] core


cdef Distogram wrap_distogram(shared_ptr[DistogramCore] core)
