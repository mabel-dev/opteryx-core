// Copyright 2018 Ulf Adams
//
// The contents of this file may be used under the terms of the Apache License,
// Version 2.0.
//
//    (See accompanying file LICENSE-Apache or copy at
//     http://www.apache.org/licenses/LICENSE-2.0)
//
// Alternatively, the contents of this file may be used under the terms of
// the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE-Boost or copy at
//     https://www.boost.org/LICENSE_1_0.txt)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
#ifndef RYU_H
#define RYU_H

// Opteryx: only the fixed-precision routines (d2fixed.c) remain vendored. The
// shortest-round-trip d2s / f2s were replaced by dragonbox (third_party/jkjeon);
// dragonbox cannot do fixed precision, so d2fixed / d2exp stay.

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>

int d2fixed_buffered_n(double d, uint32_t precision, char* result);
void d2fixed_buffered(double d, uint32_t precision, char* result);
char* d2fixed(double d, uint32_t precision);

int d2exp_buffered_n(double d, uint32_t precision, char* result);
void d2exp_buffered(double d, uint32_t precision, char* result);
char* d2exp(double d, uint32_t precision);

#ifdef __cplusplus
}
#endif

#endif // RYU_H