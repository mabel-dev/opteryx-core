#!/usr/bin/env python3
"""Benchmark NULL pointer check overhead."""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pyarrow as pa
from opteryx.compiled.draken.vectors.float64_vector import Float64Vector
from opteryx.compiled.draken.vectors.int64_vector import Int64Vector
from opteryx.compiled import vector_ops

# Create test vectors
N = 1_000_000
iterations = 10

print(f"Benchmarking with {N:,} elements, {iterations} iterations\n")

# Test vector_abs_float64
f64_data = [float(i % 1000 - 500) for i in range(N)]
f64_arrow = pa.array(f64_data, type=pa.float64())
f64_vec = Float64Vector.from_arrow(f64_arrow)

start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_abs_float64(f64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_abs_float64: {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")

# Test vector_abs_int64
i64_data = [i % 1000 - 500 for i in range(N)]
i64_arrow = pa.array(i64_data, type=pa.int64())
i64_vec = Int64Vector.from_arrow(i64_arrow)

start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_abs_int64(i64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_abs_int64:   {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")

# Test vector_sign_float64
start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_sign_float64(f64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_sign_float64: {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")

# Test vector_sign_int64
start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_sign_int64(i64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_sign_int64:   {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")

# Test vector_sqrt_float64
start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_sqrt_float64(f64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_sqrt_float64: {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")

# Test vector_sqrt_int64
start = time.perf_counter()
for _ in range(iterations):
    result = vector_ops.vector_sqrt_int64(i64_vec)
elapsed = time.perf_counter() - start
per_iter = elapsed / iterations * 1e6
throughput = N / (elapsed / iterations) / 1e6
print(f"vector_sqrt_int64:   {per_iter:.1f}µs/call, {throughput:.0f}M rows/sec")
