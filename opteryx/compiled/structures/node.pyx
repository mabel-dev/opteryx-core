# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Node Module

This module contains the Node class, which provides an implementation for dynamic
attribute management.

Noteworthy features and design choices:

1. Dynamic Attributes: The Node class allows you to set and get attributes
   dynamically, storing them in an internal dictionary.
2. Attribute Validation: Attributes starting with an underscore are not allowed.
3. Property Access: The class provides a `properties` method that exposes the internal
   attributes for external use.
4. Attribute Defaults: When attempting to access an attribute that doesn't exist, the
   `__getattr__` method will return None.
5. Deep Copy: The `copy` method allows for deep copying of the Node object, preserving
   the structure and values of all internal attributes.
6. JSON Representation: The `__str__` method returns a JSON representation of the
   internal attributes, which can be helpful for debugging or serialization.

Node accessors are one of the most frequently called functions, at the time of converting
to Cython, the shape and errors regression test suite called the getter about 850k times
during execution for about 0.2 seconds, Cython runs this class approx 33% faster that the
raw Python version.
"""

from cpython.dict cimport PyDict_Copy
from cpython cimport dict
from opteryx.compiled.functions.random_helper import random_string_c


cdef inline object _inner_copy(object obj):
    cdef type obj_type = type(obj)

    # Pointer-equality checks avoid tuple allocation of the original `in (...)` form.
    if obj_type is int or obj_type is float or obj_type is str or obj_type is bool or obj is None:
        return obj
    if obj_type is list:
        return [_inner_copy(i) for i in obj]
    if obj_type is tuple:
        return tuple(_inner_copy(i) for i in obj)
    if obj_type is dict:
        return {k: _inner_copy(v) for k, v in obj.items()}
    # hasattr is intentional here: _inner_copy handles arbitrary user objects
    # stored in node properties that implement .copy() (e.g. custom attribute types).
    # No known type can be checked statically, so this is an approved exception to §9.
    if hasattr(obj, "copy"):
        return obj.copy()
    return obj


cdef class Node:
    cdef:
        dict _properties
        object node_type
        str uuid

    def __cinit__(self, node_type, **attributes):
        """
        Initialize a new Node with a given node_type and optional attributes.
        A UUID is automatically generated for the node unless one is supplied.
        """
        self.node_type = node_type
        self.uuid = <str>attributes.pop('uuid') if 'uuid' in attributes else random_string_c(32, None)
        self._properties = dict(attributes)

    def __getattr__(self, str name):
        """
        Get an attribute by name.
        node_type and uuid are cdef slots but Python-level attribute access
        can still route through __getattr__ in certain call paths, so we
        handle them explicitly before falling through to _properties.
        """
        if name == 'node_type':
            return self.node_type
        if name == 'uuid':
            return self.uuid
        return self._properties.get(name)

    def __setattr__(self, str name, object value):
        """
        Set an attribute:
          - If name is 'node_type' or 'uuid', store directly on the object.
          - If value is None, remove it from _properties.
          - Otherwise, store in _properties.
        """
        if name == 'node_type':
            self.node_type = value
        elif name == 'uuid':
            self.uuid = value
        elif value is None:
            self._properties.pop(name, None)
        else:
            self._properties[name] = value

    @property
    def properties(self):
        """
        Return a dictionary of all node properties, including node_type and uuid.
        Dynamic attributes stored in _properties are merged.
        """
        return {
            'node_type': self.node_type,
            'uuid': self.uuid,
            **self._properties
        }

    def get(self, str name, object default=None):
        """
        Get an attribute from _properties with an optional default.
        """
        return self._properties.get(name, default)

    def __str__(self):
        """
        Return a JSON representation of the node's properties, including node_type and uuid.
        """
        from opteryx.third_party.yyjson import dumps as json_dumps
        return json_dumps(self.properties, default=str).decode('utf-8')

    def __repr__(self):
        """
        Return a string representation of the node, including its type.
        """
        node_type_str = str(self.node_type)
        if node_type_str.startswith("LogicalPlanStepType."):
            node_type_str = node_type_str[20:]
        return f"<Node type={node_type_str}>"

    cpdef Node copy(self):
        cdef Node new_node = Node(self.node_type)
        cdef object key, value

        for key, value in self._properties.items():
            new_node._properties[key] = _inner_copy(value)

        new_node.uuid = self.uuid
        return new_node

    def __reduce__(self):
        """
        Implements support for pickling (serialization).
        Returns a tuple with:
        - The class (Node)
        - The arguments needed to reconstruct the object
        - The state dictionary (optional)
        """
        return (self.__class__, (self.node_type,), self.__getstate__())

    def __getstate__(self):
        """
        Capture the state of the object as a dictionary.
        PyDict_Copy is a shallow copy — sufficient for pickling since the
        unpickler reconstructs a new dict from the serialized byte stream.
        """
        return {
            "uuid": self.uuid,
            "_properties": PyDict_Copy(self._properties)
        }

    def __setstate__(self, state):
        """
        Restore the object's state from a dictionary.
        """
        self.uuid = state["uuid"]
        self._properties = state["_properties"]
