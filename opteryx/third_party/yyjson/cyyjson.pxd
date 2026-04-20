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

    ctypedef struct yyjson_write_err:
        pass

    ctypedef struct yyjson_arr_iter:
        pass

    ctypedef struct yyjson_obj_iter:
        pass

    # Read/Write options
    ctypedef unsigned int yyjson_read_flag
    ctypedef unsigned int yyjson_write_flag
    ctypedef unsigned int yyjson_val_type

    # Write flags
    yyjson_write_flag YYJSON_WRITE_PRETTY

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

    # Array/Object value getters (size)
    size_t yyjson_arr_size(yyjson_val* arr)
    size_t yyjson_obj_size(yyjson_val* obj)

    # Array/Object get
    yyjson_val* yyjson_arr_get(yyjson_val* arr, size_t idx)
    yyjson_val* yyjson_obj_get(yyjson_val* obj, bytes key)

    # JSON Pointer
    yyjson_val* yyjson_doc_ptr_get(yyjson_doc* doc, bytes ptr)
    yyjson_val* yyjson_doc_ptr_getn(yyjson_doc* doc, bytes ptr, size_t len)
    yyjson_val* yyjson_ptr_get(yyjson_val* val, bytes ptr)
    yyjson_val* yyjson_ptr_getn(yyjson_val* val, bytes ptr, size_t len)

    # Write functions
    char* yyjson_write(const yyjson_doc* doc, yyjson_write_flag flags, size_t* len)

     # Array/Object value setters
    bint yyjson_arr_set(yyjson_val* arr, size_t idx, yyjson_val* val)
    bint yyjson_obj_set(yyjson_val* obj, bytes key, yyjson_val* val)
