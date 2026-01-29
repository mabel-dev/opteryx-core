/* Minimal shim to satisfy vendored nanobind's runtime symbol requirements.
 * These are intentionally no-op/minimal implementations to allow building
 * and importing nanobind-backed extensions without linking a separate
 * libnanobind runtime. This is a temporary, well-documented shim so we
 * can continue development; it should be replaced by the upstream runtime
 * in a follow-up change.
 */

#include <nanobind/nanobind.h>

namespace nanobind {
namespace detail {

void nb_module_exec(const char * /*domain*/, PyObject * /*m*/) {
    // No-op: skip any global initialization hooks
}

int nb_module_traverse(PyObject * /*m*/, visitproc /*visit*/, void * /*arg*/) {
    // Nothing to traverse
    return 0;
}

int nb_module_clear(PyObject * /*m*/) {
    // Nothing to clear
    return 0;
}

void nb_module_free(void * /*m*/) {
    // No resources to free here
}

} // namespace detail
} // namespace nanobind
