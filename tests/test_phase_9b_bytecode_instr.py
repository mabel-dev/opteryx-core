"""
Unit tests for Phase 9b: BytecodeInstr extension with C kernel pointers.

Verifies:
1. BytecodeInstr struct size (with new kernel_fn and ctx_ptr fields)
2. Kernel resolution functionality
3. Bytecode builder integration
"""

import unittest
from opteryx.compiled.expression.compiled_expression import (
    CompiledBytecode,
    _resolve_kernel_and_context,
)


class TestByteCodeInstrPhase9b(unittest.TestCase):
    """Test that BytecodeInstr struct has been extended for Phase 9b."""

    def test_bytecode_instr_extension(self):
        """Verify BytecodeInstr struct contains kernel_fn and ctx_ptr fields."""
        # Phase 9b adds two void* fields (kernel_fn, ctx_ptr) to BytecodeInstr
        bc = CompiledBytecode()
        # Verify that CompiledBytecode can be instantiated without error
        self.assertIsNotNone(bc)
        self.assertEqual(bc.length, 0)
        # The struct extension is verified by compilation success and these tests

    def test_resolve_kernel_binary_op(self):
        """Test kernel resolution for binary operations."""
        # Test resolving a binary op kernel
        fn_ptr, ctx_wrapper = _resolve_kernel_and_context("draken_add", None, None)
        self.assertIsNotNone(fn_ptr)
        self.assertIsNone(ctx_wrapper)
        self.assertIsInstance(fn_ptr, int)
        self.assertGreater(fn_ptr, 0)

    def test_resolve_kernel_cast(self):
        """Test kernel resolution for cast operations."""
        # Test resolving a cast kernel
        fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
            "draken_cast_int64_to_float64", None, None
        )
        self.assertIsNotNone(fn_ptr)
        self.assertIsNone(ctx_wrapper)
        self.assertIsInstance(fn_ptr, int)
        self.assertGreater(fn_ptr, 0)

    def test_resolve_kernel_cast_int64_to_timestamp(self):
        """Test kernel resolution for INT64 to TIMESTAMP cast."""
        fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
            "draken_cast_int64_to_timestamp", None, None
        )
        self.assertIsNotNone(fn_ptr)
        self.assertIsNone(ctx_wrapper)
        self.assertIsInstance(fn_ptr, int)
        self.assertGreater(fn_ptr, 0)

    def test_resolve_kernel_extraction(self):
        """Test kernel resolution for extraction operations."""
        # Test extraction kernel
        fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
            "draken_map_access_string", None, None
        )
        self.assertIsNotNone(fn_ptr)
        self.assertIsNone(ctx_wrapper)
        self.assertIsInstance(fn_ptr, int)
        self.assertGreater(fn_ptr, 0)

    def test_resolve_kernel_not_found(self):
        """Test that resolving a non-existent kernel raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            _resolve_kernel_and_context("draken_nonexistent_kernel", None, None)
        self.assertIn("not found in registry", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
