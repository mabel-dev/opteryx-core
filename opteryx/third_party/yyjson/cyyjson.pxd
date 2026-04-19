"""Cython declarations for yyjson."""

cdef extern from "yyjson.h":
    # Opaque types
    ctypedef struct yyjson_doc:
        pass

    ctypedef struct yyjson_val:
        pass

    ctypedef struct yyjson_alc:
        pass

    ctypedef struct yyjson_read_err:
        pass

    ctypedef struct yyjson_arr_iter:
        pass

    ctypedef struct yyjson_obj_iter:
        pass

    # Read/Write options
    ctypedef unsigned int yyjson_read_flag
    ctypedef unsigned int yyjson_write_flag

    # Read functions
    yyjson_doc* yyjson_read_opts(char* data, size_t len,
                                  yyjson_read_flag flags,
                                  const yyjson_alc* alc,
                                  yyjson_read_err* err)

    yyjson_val* yyjson_doc_get_root(yyjson_doc* doc)

    void yyjson_doc_free(yyjson_doc* doc)

    void yyjson_free(void* ptr)

    # Value access predicates
    bint yyjson_is_null(yyjson_val* val)
    bint yyjson_is_bool(yyjson_val* val)
    bint yyjson_is_uint(yyjson_val* val)
    bint yyjson_is_sint(yyjson_val* val)
    bint yyjson_is_int(yyjson_val* val)
    bint yyjson_is_real(yyjson_val* val)
    bint yyjson_is_num(yyjson_val* val)
    bint yyjson_is_str(yyjson_val* val)
    bint yyjson_is_arr(yyjson_val* val)
    bint yyjson_is_obj(yyjson_val* val)

    # Value getters
    bint yyjson_get_bool(yyjson_val* val)
    unsigned long long yyjson_get_uint(yyjson_val* val)
    long long yyjson_get_sint(yyjson_val* val)
    double yyjson_get_real(yyjson_val* val)
    const char* yyjson_get_str(yyjson_val* val)
    size_t yyjson_get_len(yyjson_val* val)

    # Array iteration
    yyjson_arr_iter yyjson_arr_iter_with(yyjson_val* arr)
    yyjson_val* yyjson_arr_iter_next(yyjson_arr_iter* iter)

    # Object iteration
    yyjson_obj_iter yyjson_obj_iter_with(yyjson_val* obj)
    yyjson_val* yyjson_obj_iter_next(yyjson_obj_iter* iter)
    yyjson_val* yyjson_obj_iter_get_val(yyjson_val* key)
