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

enum Algorithm : std::uint8_t {
    None = 0x00,
    LZ4 = 0x01
};

enum class Compressed : std::uint8_t {
    On,
    Off
};

/** Decompress input stream.
 * @tparam InputStream ZeroCopyInputStream
 * @tparam BufferFactory Callable object or lambda.
 *    Takes the requested buffer size and returns allocated buffer pointer.
 * @param in Input source stream
 * @param inSize Size of compressed data
 * @param bf Decompressed buffer allocator
 * @param algorithm Compression algorithm type
 * @return Size of decompressed data or zero if failed to decompress
 */
template<typename InputStream, typename BufferFactory>
std::size_t
decompress(InputStream& in,
           std::size_t in_size, BufferFactory&& bf, Algorithm algorithm = Algorithm::LZ4) {
    try
    {
        if (algorithm == Algorithm::LZ4)
            return ripple::compression_algorithms::lz4fDecompress(in, in_size, std::forward<BufferFactory>(bf));
        else
            Throw<std::runtime_error>(
                    "decompress: invalid compression algorithm");
    }
    catch (...)
    {
        return 0;
    }
}

/** Compress input data.
 * @tparam BufferFactory Callable object or lambda.
 *     Takes the requested buffer size and returns allocated buffer pointer.
 * @param in Data to compress
 * @param inSize Size of the data
 * @param bf Compressed buffer allocator
 * @param algorithm Compression algorithm type
 * @return Size of compressed data, or zero if failed to compress
 */
template<class BufferFactory>
std::size_t
compress(void const* in,
         std::size_t in_size, BufferFactory&& bf, Algorithm algorithm = Algorithm::LZ4) {
    try
    {
        if (algorithm == Algorithm::LZ4)
            return ripple::compression_algorithms::lz4fCompress(in, in_size, std::forward<BufferFactory>(bf));
        else
            Throw<std::runtime_error>(
                    "compress: invalid compression algorithm");
    }
    catch (...)
    {
        return 0;
    }
}
} // compression

} // ripple

#endif //RIPPLED_COMPRESSION_H_INCLUDED
