//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2020 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#ifndef RIPPLED_COMPRESSION_H_INCLUDED
#define RIPPLED_COMPRESSION_H_INCLUDED

#include <ripple/basics/CompressionAlgorithms.h>
#include <lz4frame.h>

namespace ripple {

namespace compression {

enum Algorithm : uint8_t {
    LZ4 = 0x01
};

template<typename InputStream, typename BufferFactory>
std::pair<void const *, std::size_t>
decompress(InputStream& in,
           std::size_t in_size, BufferFactory &&bf, uint8_t algorithm = Algorithm::LZ4) {
    if (algorithm == Algorithm::LZ4)
        return ripple::compression_algorithms::lz4f_decompress(in, in_size, bf);
    else
        Throw<std::runtime_error>(
                "decompress: invalid compression algorithm");
}

template<class BufferFactory>
std::pair<void const *, std::size_t>
compress(void const *in,
         std::size_t in_size, BufferFactory &&bf, uint8_t algorithm = Algorithm::LZ4) {
    if (algorithm == Algorithm::LZ4)
        return ripple::compression_algorithms::lz4f_compress(in, in_size, bf);
    else
        Throw<std::runtime_error>(
                "compress: invalid compression algorithm");
}
} // compression

} // ripple

#endif //RIPPLED_COMPRESSION_H_INCLUDED
