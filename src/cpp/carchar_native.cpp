#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/vector.h>

#include <cstdint>

#include "carchar.hpp"

namespace nb = nanobind;
using opteryx::carchar::CarcharIndex;
using opteryx::carchar::CarcharJoinIndex;
using opteryx::carchar::CarcharJoinEngine;
using opteryx::carchar::CarcharStats;

namespace {

struct BufferView {
    Py_buffer view {};
    bool acquired = false;

    ~BufferView() {
        if (acquired) {
            PyBuffer_Release(&view);
        }
    }
};

void acquire_buffer(nb::handle obj, int flags, BufferView& out, const char* err_msg) {
    if (PyObject_GetBuffer(obj.ptr(), &out.view, flags) != 0) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, err_msg);
        }
        throw nb::python_error();
    }
    out.acquired = true;
}

std::uint64_t sum_probe_counts(CarcharJoinIndex& index, nb::handle keys_obj) {
    BufferView keys_view;
    acquire_buffer(keys_obj, PyBUF_SIMPLE, keys_view, "keys object does not support buffer protocol");

    if (keys_view.view.len % static_cast<Py_ssize_t>(sizeof(std::uint64_t)) != 0) {
        throw nb::value_error("keys buffer must contain packed uint64 values");
    }

    const auto* keys = static_cast<const std::uint64_t*>(keys_view.view.buf);
    const auto length = static_cast<std::size_t>(keys_view.view.len / sizeof(std::uint64_t));
    return index.probe_row_count_sum(keys, length);
}

std::uint64_t sum_probe_counts_engine(CarcharJoinEngine& engine, nb::handle keys_obj) {
    BufferView keys_view;
    acquire_buffer(keys_obj, PyBUF_SIMPLE, keys_view, "keys object does not support buffer protocol");

    if (keys_view.view.len % static_cast<Py_ssize_t>(sizeof(std::uint64_t)) != 0) {
        throw nb::value_error("keys buffer must contain packed uint64 values");
    }

    const auto* keys = static_cast<const std::uint64_t*>(keys_view.view.buf);
    const auto length = static_cast<std::size_t>(keys_view.view.len / sizeof(std::uint64_t));
    return engine.probe_row_count_sum(keys, length);
}

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> probe_join_indices_engine(
    CarcharJoinEngine& engine,
    nb::handle keys_obj,
    nb::handle probe_rows_obj
) {
    BufferView keys_view;
    BufferView probe_rows_view;
    acquire_buffer(keys_obj, PyBUF_SIMPLE, keys_view, "keys object does not support buffer protocol");
    acquire_buffer(
        probe_rows_obj,
        PyBUF_SIMPLE,
        probe_rows_view,
        "probe_rows object does not support buffer protocol"
    );

    if (keys_view.view.len % static_cast<Py_ssize_t>(sizeof(std::uint64_t)) != 0) {
        throw nb::value_error("keys buffer must contain packed uint64 values");
    }
    if (probe_rows_view.view.len % static_cast<Py_ssize_t>(sizeof(std::int64_t)) != 0) {
        throw nb::value_error("probe_rows buffer must contain packed int64 values");
    }

    const auto key_count = static_cast<std::size_t>(keys_view.view.len / sizeof(std::uint64_t));
    const auto probe_row_count =
        static_cast<std::size_t>(probe_rows_view.view.len / sizeof(std::int64_t));
    if (key_count != probe_row_count) {
        throw nb::value_error("keys and probe_rows must have the same length");
    }

    return engine.probe_join_indices(
        static_cast<const std::uint64_t*>(keys_view.view.buf),
        static_cast<const std::int64_t*>(probe_rows_view.view.buf),
        key_count
    );
}

void insert_batch_with_row_ids(CarcharJoinEngine& engine, nb::handle keys_obj, nb::handle row_ids_obj) {
    BufferView keys_view;
    BufferView row_ids_view;
    acquire_buffer(keys_obj, PyBUF_SIMPLE, keys_view, "keys object does not support buffer protocol");
    acquire_buffer(row_ids_obj, PyBUF_SIMPLE, row_ids_view, "row_ids object does not support buffer protocol");

    if (keys_view.view.len % static_cast<Py_ssize_t>(sizeof(std::uint64_t)) != 0) {
        throw nb::value_error("keys buffer must contain packed uint64 values");
    }
    if (row_ids_view.view.len % static_cast<Py_ssize_t>(sizeof(std::int64_t)) != 0) {
        throw nb::value_error("row_ids buffer must contain packed int64 values");
    }

    const auto key_count = static_cast<std::size_t>(keys_view.view.len / sizeof(std::uint64_t));
    const auto row_count = static_cast<std::size_t>(row_ids_view.view.len / sizeof(std::int64_t));
    if (key_count != row_count) {
        throw nb::value_error("keys and row_ids must have the same length");
    }

    engine.insert_batch(
        static_cast<const std::uint64_t*>(keys_view.view.buf),
        static_cast<const std::int64_t*>(row_ids_view.view.buf),
        key_count
    );
}

void insert_batch_with_ordinal_rows(CarcharJoinEngine& engine, nb::handle keys_obj) {
    BufferView keys_view;
    acquire_buffer(keys_obj, PyBUF_SIMPLE, keys_view, "keys object does not support buffer protocol");

    if (keys_view.view.len % static_cast<Py_ssize_t>(sizeof(std::uint64_t)) != 0) {
        throw nb::value_error("keys buffer must contain packed uint64 values");
    }

    const auto* keys = static_cast<const std::uint64_t*>(keys_view.view.buf);
    const auto length = static_cast<std::size_t>(keys_view.view.len / sizeof(std::uint64_t));
    engine.insert_batch(keys, length);
}

}  // namespace

NB_MODULE(carchar_native, m) {
    nb::class_<CarcharStats>(m, "CarcharStats")
        .def_ro("capacity", &CarcharStats::capacity)
        .def_ro("size", &CarcharStats::size)
        .def_ro("resize_count", &CarcharStats::resize_count)
        .def_ro("lookup_count", &CarcharStats::lookup_count)
        .def_ro("insert_count", &CarcharStats::insert_count)
        .def_ro("total_probes", &CarcharStats::total_probes)
        .def_ro("max_probe_length", &CarcharStats::max_probe_length)
        .def_ro("lookup_total_probes", &CarcharStats::lookup_total_probes)
        .def_ro("insert_total_probes", &CarcharStats::insert_total_probes)
        .def_ro("max_lookup_probe_length", &CarcharStats::max_lookup_probe_length)
        .def_ro("max_insert_probe_length", &CarcharStats::max_insert_probe_length)
        .def_ro("bytes_estimate", &CarcharStats::bytes_estimate)
        .def_prop_ro("load_factor", [](const CarcharStats& stats) { return stats.load_factor(); })
        .def_prop_ro("average_probe_length",
                     [](const CarcharStats& stats) { return stats.average_probe_length(); })
        .def_prop_ro("average_lookup_probe_length",
                     [](const CarcharStats& stats) { return stats.average_lookup_probe_length(); })
        .def_prop_ro("average_insert_probe_length",
                     [](const CarcharStats& stats) { return stats.average_insert_probe_length(); });

    nb::class_<CarcharIndex>(m, "CarcharIndex")
        .def(nb::init<std::size_t, double>(), nb::arg("initial_capacity") = 16,
             nb::arg("load_factor") = 0.80)
        .def("reserve", &CarcharIndex::reserve, nb::arg("expected_entries"))
        .def("size", &CarcharIndex::size)
        .def("capacity", &CarcharIndex::capacity)
        .def("lookup",
             [](CarcharIndex& self, std::uint64_t key) -> nb::object {
                 std::int64_t payload_ref = -1;
                 if (!self.lookup(key, payload_ref)) {
                     return nb::none();
                 }
                 return nb::int_(payload_ref);
             },
             nb::arg("key"))
        .def("insert_new", &CarcharIndex::insert_new, nb::arg("key"), nb::arg("payload_ref"))
        .def(
            "items",
            [](const CarcharIndex& self) { return self.items(); },
            "Return all occupied (key, payload_ref) items"
        )
        .def("stats", &CarcharIndex::stats);

    nb::class_<CarcharJoinIndex>(m, "CarcharJoinIndex")
        .def(nb::init<std::size_t, double>(), nb::arg("initial_capacity") = 16,
             nb::arg("load_factor") = 0.80)
        .def("reserve", &CarcharJoinIndex::reserve, nb::arg("expected_entries"))
        .def("size", &CarcharJoinIndex::size)
        .def("capacity", &CarcharJoinIndex::capacity)
        .def("insert_row", &CarcharJoinIndex::insert_row, nb::arg("key"), nb::arg("row_id"))
        .def("append_join_row", &CarcharJoinIndex::append_join_row, nb::arg("payload_ref"),
             nb::arg("row_id"))
        .def("rows_for", &CarcharJoinIndex::rows_for, nb::arg("key"))
        .def("row_count_for", &CarcharJoinIndex::row_count_for, nb::arg("key"))
        .def("probe_row_count_sum", &sum_probe_counts, nb::arg("keys"))
        .def("rows_from_payload", &CarcharJoinIndex::rows_from_payload, nb::arg("payload_ref"))
        .def("get", &CarcharJoinIndex::get, nb::arg("key"))
        .def("stats", &CarcharJoinIndex::stats);

    nb::class_<CarcharJoinEngine>(m, "CarcharJoinEngine")
        .def(nb::init<std::size_t, std::size_t, double, double>(), nb::arg("expected_entries") = 0,
             nb::arg("partition_bits") = 0, nb::arg("load_factor") = 0.80,
             nb::arg("probe_load_factor") = 0.80)
        .def("reserve", &CarcharJoinEngine::reserve, nb::arg("expected_entries"))
        .def("seal", &CarcharJoinEngine::seal)
        .def("size", &CarcharJoinEngine::size)
        .def("capacity", &CarcharJoinEngine::capacity)
        .def("partition_bits", &CarcharJoinEngine::partition_bits)
        .def("partition_count", &CarcharJoinEngine::partition_count)
        .def("insert_row", &CarcharJoinEngine::insert_row, nb::arg("key"), nb::arg("row_id"))
        .def("insert_batch", &insert_batch_with_row_ids, nb::arg("keys"), nb::arg("row_ids"))
        .def("insert_keys", &insert_batch_with_ordinal_rows, nb::arg("keys"))
        .def("append_join_row", &CarcharJoinEngine::append_join_row, nb::arg("payload_ref"),
             nb::arg("row_id"))
        .def("rows_for", &CarcharJoinEngine::rows_for, nb::arg("key"))
        .def("row_count_for", &CarcharJoinEngine::row_count_for, nb::arg("key"))
        .def("probe_row_count_sum", &sum_probe_counts_engine, nb::arg("keys"))
        .def("probe_join_indices", &probe_join_indices_engine, nb::arg("keys"), nb::arg("probe_rows"))
        .def("rows_from_payload", &CarcharJoinEngine::rows_from_payload, nb::arg("payload_ref"))
        .def("get", &CarcharJoinEngine::get, nb::arg("key"))
        .def("stats", &CarcharJoinEngine::stats);
}
