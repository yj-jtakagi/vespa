// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/fastos/dynamiclibrary.h>
#include <cstring>

namespace vespalib::hwaccelrated::avx {

namespace {

inline bool validAlignment(const void * p, const size_t align) {
    return (reinterpret_cast<uint64_t>(p) & (align-1)) == 0;
}

template <typename T, typename V>
T sumT(const V & v) {
    T sum(0);
    for (size_t i(0); i < (sizeof(V)/sizeof(T)); i++) {
        sum += v[i];
    }
    return sum;
}

template <typename R, typename T, size_t C>
R sumR(const T * v) {
    if (C == 1) {
        return v[0].v;
    } else if (C == 2) {
        return v[0].v + v[1].v;
    } else {
        return sumR<R, T, C/2>(v) + sumR<R, T, C/2>(v+C/2);
    }
}

template <typename T, size_t N>
union U;

template <typename T>
union U<T, 64u> {
    T __attribute__((vector_size(64)))  v;
    T __attribute__((vector_size(32)))  v2[2];
    T __attribute__((vector_size(16)))  v4[4];
};

template <typename T>
union U<T, 32u> {
    T __attribute__((vector_size(32)))  v;
    T __attribute__((vector_size(16)))  v2[2];
};

template <typename T, typename UV>
T sumU(const UV & v) {
    return sumT<T>(v.v);
};

template <typename T>
T sumU(U<T, 64u> & v) {
    v.v2[0] += v.v2[1];
    v.v4[0] += v.v4[1];
    return sumT<T>(v.v4[0]);
}

template <typename T>
T sumU(U<T, 32u> & v) {
    v.v2[0] += v.v2[1];
    return sumT<T>(v.v2[0]);
}

template <typename T, size_t VLEN, unsigned AlignA, unsigned AlignB, size_t VectorsPerChunk>
static T computeDotProduct(const T * af, const T * bf, size_t sz) __attribute__((noinline));

template <typename T, size_t VLEN, unsigned AlignA, unsigned AlignB, size_t VectorsPerChunk>
T computeDotProduct(const T * af, const T * bf, size_t sz)
{
    constexpr const size_t ChunkSize = VLEN*VectorsPerChunk/sizeof(T);
    typedef T V __attribute__ ((vector_size (VLEN)));
    using UV = U<T, VLEN>;
    typedef T A __attribute__ ((vector_size (VLEN), aligned(AlignA)));
    typedef T B __attribute__ ((vector_size (VLEN), aligned(AlignB)));
    UV partial[VectorsPerChunk];
    memset(partial, 0, sizeof(partial));
    const A * a = reinterpret_cast<const A *>(af);
    const B * b = reinterpret_cast<const B *>(bf);

    const size_t numChunks(sz/ChunkSize);
    for (size_t i(0); i < numChunks; i++) {
        for (size_t j(0); j < VectorsPerChunk; j++) {
            partial[j].v += a[VectorsPerChunk*i+j] * b[VectorsPerChunk*i+j];
        }
    }
    for (size_t i(numChunks*VectorsPerChunk); i < sz*sizeof(T)/VLEN; i++) {
        partial[0].v += a[i] * b[i];
    }
    T sum(0);
    for (size_t i = (sz/(VLEN/sizeof(T)))*(VLEN/sizeof(T)); i < sz; i++) {
        sum += af[i] * bf[i];
    }
    partial[0].v = sumR<V, UV, VectorsPerChunk>(partial);

    return sum + sumU<T>(partial[0]);
}

}

template <typename T, size_t VLEN, size_t VectorsPerChunk=4>
VESPA_DLL_LOCAL T dotProductSelectAlignment(const T * af, const T * bf, size_t sz);

template <typename T, size_t VLEN, size_t VectorsPerChunk>
T dotProductSelectAlignment(const T * af, const T * bf, size_t sz)
{
    if (validAlignment(af, VLEN)) {
        if (validAlignment(bf, VLEN)) {
            return computeDotProduct<T, VLEN, VLEN, VLEN, VectorsPerChunk>(af, bf, sz);
        } else {
            return computeDotProduct<T, VLEN, VLEN, 1, VectorsPerChunk>(af, bf, sz);
        }
    } else {
        if (validAlignment(bf, VLEN)) {
            return computeDotProduct<T, VLEN, 1, VLEN, VectorsPerChunk>(af, bf, sz);
        } else {
            return computeDotProduct<T, VLEN, 1, 1, VectorsPerChunk>(af, bf, sz);
        }
    }
}

}
