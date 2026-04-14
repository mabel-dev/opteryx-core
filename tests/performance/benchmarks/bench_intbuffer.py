import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.buffers import IntBuffer


def bench_append(N, iterations=10):
    b = IntBuffer(size_hint=N)
    # warmup
    for i in range(1000):
        b.append(i)

    times = []
    for _ in range(iterations):
        b = IntBuffer(size_hint=N)
        t0 = time.perf_counter()
        for i in range(N):
            b.append(i)
        t1 = time.perf_counter()
        times.append(t1 - t0)

    return min(times), sum(times) / len(times)


def bench_extend(N, chunk=10000, iterations=10):
    # prepare chunks
    chunks = [list(range(chunk)) for _ in range(max(1, N // chunk))]

    times = []
    for _ in range(iterations):
        b = IntBuffer(size_hint=N)
        t0 = time.perf_counter()
        for c in chunks:
            b.extend(c)
        t1 = time.perf_counter()
        times.append(t1 - t0)

    return min(times), sum(times) / len(times)


def bench_to_int64_vector(N, iterations=10):
    b = IntBuffer(size_hint=N)
    for i in range(N):
        b.append(i)

    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        _ = b.to_int64_vector()
        t1 = time.perf_counter()
        times.append(t1 - t0)

    return min(times), sum(times) / len(times)


def bench_python_list(N, iterations=5):
    # baseline: python list append
    times_append = []
    for _ in range(iterations):
        lst = []
        t0 = time.perf_counter()
        for i in range(N):
            lst.append(i)
        t1 = time.perf_counter()
        times_append.append(t1 - t0)

    return (min(times_append), sum(times_append) / len(times_append))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=2_000_000, help='number of elements')
    parser.add_argument('--iterations', type=int, default=10)
    args = parser.parse_args()

    N = args.n
    iters = args.iterations
    print('IntBuffer benchmark — N=', N)

    print('\nBenchmarking append...')
    best, mean = bench_append(N, iterations=iters)
    print(f'append: best={best:.4f}s mean={mean:.4f}s')

    print('\nBenchmarking extend (chunks of 50000)...')
    best, mean = bench_extend(N, chunk=50000, iterations=iters)
    print(f'extend: best={best:.4f}s mean={mean:.4f}s')

    print('\nBenchmarking to_int64_vector...')
    best, mean = bench_to_int64_vector(N, iterations=iters)
    print(f'to_int64_vector: best={best:.4f}s mean={mean:.4f}s')

    print('\nPython list baseline (append)...')
    a_best, a_mean = bench_python_list(N, iterations=iters)
    print(f'py_append: best={a_best:.4f}s mean={a_mean:.4f}s')
