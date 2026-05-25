# cython: language_level=3
# Cython shim for draken.morsels.morsel — E.24 vtable bridge.

from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector


cdef inline object _unwrap(object v):
    """Return the raw nanobind VectorOwner from either a Vector or a raw nb vector."""
    if isinstance(v, Vector):
        return (<Vector>v)._nb
    return v


cdef inline Vector _wrap(object v):
    if isinstance(v, Vector):
        return <Vector>v
    return Vector(v)


cdef Morsel _make_morsel():
    cdef Morsel m = Morsel.__new__(Morsel)
    from draken.draken_native import Morsel as NbMorsel
    m._nb = NbMorsel()
    m._col_names = []
    m._columns = []
    return m


cdef class Morsel:
    def __cinit__(self, object nb_morsel=None):
        if nb_morsel is None:
            from draken.draken_native import Morsel as NbMorsel
            self._nb = NbMorsel()
        else:
            self._nb = nb_morsel
        self._col_names = []
        self._columns = []

    def __len__(self):
        return len(self._columns)

    def __getitem__(self, int idx):
        return self._columns[idx]

    @property
    def num_rows(self):
        if not self._columns:
            return 0
        return (<Vector>self._columns[0]).length

    @property
    def ptr(self):
        # evaluation.pyx accesses morsel.ptr.num_rows — satisfy via self
        return self

    @property
    def nbytes(self):
        return self.num_rows * len(self._col_names) * 8

    @property
    def num_columns(self):
        return len(self._columns)

    @property
    def column_names(self):
        return self._col_names

    @property
    def column_types(self):
        return [(<Vector>self._columns[i]).type for i in range(len(self._columns))]

    def hash(self, col_names):
        # Returns uint64_t[::1] row hashes for the given columns.
        from array import array as _array
        cdef Py_ssize_t n = self.num_rows
        if n == 0:
            return _array('Q', [])
        hashes = [0x9e3779b97f4a7c15] * n  # seed
        for name in col_names:
            if isinstance(name, str):
                name = name.encode("utf-8")
            vec = None
            for i, col_name in enumerate(self._col_names):
                if col_name == name:
                    vec = <Vector>self._columns[i]
                    break
            if vec is None:
                continue
            col_hashes = vec._nb.hash()
            for i in range(n):
                # Mix: FNV-style combining
                hashes[i] = ((hashes[i] ^ int(col_hashes[i])) * 0xbf58476d1ce4e5b9) & 0xFFFFFFFFFFFFFFFF
        return _array('Q', hashes)

    def _column_index_from_name(self, name):
        if isinstance(name, str):
            name = name.encode("utf-8")
        for i, n in enumerate(self._col_names):
            if n == name:
                return i
        raise KeyError(f"_column_index_from_name: column not found: {name!r}")

    def _ensure_name_map(self):
        return {name: i for i, name in enumerate(self._col_names)}

    def append(self, vec):
        cdef Vector wrapped = _wrap(vec)
        self._nb.append(wrapped._nb)
        self._columns.append(wrapped)

    def append_vector(self, name, vec):
        if isinstance(name, str):
            name = name.encode("utf-8")
        cdef Vector wrapped = _wrap(vec)
        self._nb.append(wrapped._nb)
        self._col_names.append(name)
        self._columns.append(wrapped)

    def column(self, name, fallback=None):
        if isinstance(name, str):
            name = name.encode("utf-8")
        for i, n in enumerate(self._col_names):
            if n == name:
                return self._columns[i]
        if fallback is not None:
            if isinstance(fallback, str):
                fallback = fallback.encode("utf-8")
            for i, n in enumerate(self._col_names):
                if n == fallback:
                    return self._columns[i]
        raise KeyError(f"column not found: {name!r}")

    def select(self, col_names):
        cdef Morsel result = _make_morsel()
        for name in col_names:
            if isinstance(name, str):
                name = name.encode("utf-8")
            for i, n in enumerate(self._col_names):
                if n == name:
                    result._nb.append((<Vector>self._columns[i])._nb)
                    result._col_names.append(n)
                    result._columns.append(self._columns[i])
                    break
        return result

    def rename(self, new_names):
        cdef Morsel result = _make_morsel()
        for i, name in enumerate(new_names):
            if isinstance(name, str):
                name = name.encode("utf-8")
            result._nb.append((<Vector>self._columns[i])._nb)
            result._col_names.append(name)
            result._columns.append(self._columns[i])
        return result

    def filter_mask(self, mask):
        # mask is a BoolVector; extract True indices then take
        cdef list idx_list = [i for i, v in enumerate(mask.to_pylist()) if v]
        return self.take(idx_list)

    def take(self, indices):
        cdef Morsel result = _make_morsel()
        result._col_names = list(self._col_names)
        cdef int n = len(self._columns)
        idx_list = list(indices)
        for i in range(n):
            nb_taken = (<Vector>self._columns[i])._nb.take(idx_list)
            result._nb.append(nb_taken)
            result._columns.append(Vector(nb_taken))
        return result

    def slice(self, Py_ssize_t offset=0, Py_ssize_t length=0, Py_ssize_t start=-1):
        cdef Py_ssize_t real_start = start if start >= 0 else offset
        return self.take(range(real_start, real_start + length))

    def copy(self, columns=None, mask=None):
        cdef Morsel result = _make_morsel()
        cdef int n = len(self._columns)
        if columns is not None:
            col_set = []
            for c in columns:
                if isinstance(c, str):
                    c = c.encode("utf-8")
                col_set.append(c)
            for i in range(n):
                if self._col_names[i] in col_set:
                    if mask is not None:
                        nb_v = (<Vector>self._columns[i])._nb.take(mask)
                        result._nb.append(nb_v)
                        result._columns.append(Vector(nb_v))
                    else:
                        result._nb.append((<Vector>self._columns[i])._nb)
                        result._columns.append(self._columns[i])
                    result._col_names.append(self._col_names[i])
        elif mask is not None:
            result._col_names = list(self._col_names)
            for i in range(n):
                nb_v = (<Vector>self._columns[i])._nb.take(mask)
                result._nb.append(nb_v)
                result._columns.append(Vector(nb_v))
        else:
            result._col_names = list(self._col_names)
            for i in range(n):
                result._nb.append((<Vector>self._columns[i])._nb)
                result._columns.append(self._columns[i])
        return result

    @classmethod
    def combine(cls, morsels):
        """Vertical concatenation of multiple morsels with the same schema."""
        if not morsels:
            return _make_morsel()
        first = <Morsel>morsels[0]
        if len(morsels) == 1:
            return first
        from draken.draken_native import (
            vector_from_sequence as _nb_int64,
            vector_from_string_sequence as _nb_varchar,
            vector_from_bool_sequence as _nb_bool,
            vector_float64_from_sequence as _nb_float64,
            vector_timestamp_from_sequence as _nb_timestamp,
            vector_date32_from_sequence as _nb_date32,
        )
        _TYPE_FACTORIES = {
            "INT8": _nb_int64, "INT16": _nb_int64, "INT32": _nb_int64, "INT64": _nb_int64,
            "FLOAT32": _nb_float64, "FLOAT64": _nb_float64,
            "BOOL": _nb_bool,
            "VARCHAR": _nb_varchar, "NVARCHAR": _nb_varchar, "VARBINARY": _nb_varchar, "DICTIONARY": _nb_varchar,
            "TIMESTAMP64": _nb_timestamp,
            "DATE32": _nb_date32,
        }
        cdef Morsel result = _make_morsel()
        result._col_names = list(first._col_names)
        cdef int ncols = len(first._columns)
        for col_idx in range(ncols):
            combined_data = []
            for m in morsels:
                combined_data.extend((<Vector>(<Morsel>m)._columns[col_idx])._nb.to_pylist())
            type_name = (<Vector>first._columns[col_idx]).type.name
            factory = _TYPE_FACTORIES.get(type_name)
            if factory is None:
                raise NotImplementedError(f"Morsel.combine: no factory for type {type_name!r}")
            if type_name == "TIMESTAMP64":
                nb_v = factory(combined_data, "us")
            else:
                nb_v = factory(combined_data)
            result._nb.append(nb_v)
            result._columns.append(Vector(nb_v))
        return result

    @classmethod
    def from_vectors(cls, col_names, col_vecs):
        from draken.draken_native import Morsel as NbMorsel
        cdef Morsel result = cls.__new__(cls)
        cdef Vector wrapped
        result._nb = NbMorsel()
        result._col_names = []
        result._columns = []
        for name, vec in zip(col_names, col_vecs):
            if isinstance(name, str):
                name = name.encode("utf-8")
            wrapped = _wrap(vec)
            result._nb.append(wrapped._nb)
            result._col_names.append(name)
            result._columns.append(wrapped)
        return result
