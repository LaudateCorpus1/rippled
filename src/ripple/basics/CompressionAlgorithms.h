//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#ifndef RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED
#define RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED

#include <ripple/basics/varint_common.h>
#include <lz4frame.h>

namespace ripple {

namespace compression_algorithms {

inline void do_throw(const char *message)
{
    Throw<std::runtime_error>(message);
}

template<typename BufferFactory>
std::pair<void const*, std::size_t>
lz4f_compress(void const * in,
              std::size_t in_size, BufferFactory&& bf)
{
    std::pair<void const*, std::size_t> result;
    uint8_t const original_size_bytes = 4;

    std::size_t const out_capacity = LZ4F_compressFrameBound(in_size, NULL);

    std::uint8_t* compressed = reinterpret_cast<std::uint8_t*>(
            bf(original_size_bytes + out_capacity));
    result.first = compressed;

    std::memcpy(compressed,
            reinterpret_cast<std::uint8_t*>(&in_size), original_size_bytes);

    size_t compressed_size = LZ4F_compressFrame(
            compressed + original_size_bytes,
            out_capacity,
            in,
            in_size,
            NULL);
    if (LZ4F_isError(compressed_size))
        do_throw("lz4f failed compress update");

    result.second = original_size_bytes + compressed_size;

    return result;
}

template<typename BufferIn, typename BufferFactory>
std::pair<void const*, std::size_t>
lz4f_decompress(BufferIn&& in,
                std::size_t in_size, BufferFactory&& bf)
{
    std::pair<void const*, std::size_t> result{0, 0};
    LZ4F_dctx_s* dctx;
    uint32_t original_size = 0;
    uint8_t original_size_bytes = 4;
    void const * compressed_chunk = NULL;
    std::size_t chunk_size = 0;
    std::size_t copied = 0;
    std::size_t decompressed_size = 0;
    std::uint8_t * decompressed = NULL;
    bool done = false;
    bool done_header = false;

    std::size_t const dctx_status = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(dctx_status))
        do_throw("lz4f failed decompression context");

    while (!done && in(compressed_chunk, chunk_size))
    {
        if (!done_header)
        {
            auto sz =
                    (chunk_size >= (original_size_bytes - copied))
                    ? (original_size_bytes - copied)
                    : chunk_size;
            std::memcpy(reinterpret_cast<std::uint8_t*>(&original_size) + copied,
                    compressed_chunk, sz);
            copied += sz;
            if (copied == original_size_bytes)
            {
                decompressed = bf(original_size);
                result.first = decompressed;
                result.second = original_size;
                compressed_chunk = reinterpret_cast<std::uint8_t const*>(
                                           compressed_chunk) + sz;
                chunk_size -= sz;
                done_header = true;
            }
            else
                continue;
        }

        if (chunk_size == 0)
            continue;

        std::size_t dst_size = result.second - decompressed_size;
        chunk_size = (chunk_size <= in_size) ? chunk_size : in_size;
        std::size_t src_size = chunk_size;
        size_t size = LZ4F_decompress(dctx,
                                      decompressed + decompressed_size,
                                      &dst_size,
                                      compressed_chunk,
                                      &src_size,
                                      NULL);
        if (LZ4F_isError(size))
            do_throw("lz4f failed decompress");
        // TODO decompress might not consume all source
        // should concat the remainder with the next chunk
        assert (src_size == chunk_size);
        decompressed_size += dst_size;
        in_size -= src_size;
        done = decompressed_size == result.second;
    }

    return result;
}

} // compression

} // ripple

#endif //RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED
