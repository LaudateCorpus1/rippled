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

#ifndef RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED
#define RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED

#include <ripple/nodestore/impl/varint.h>
#include <lz4frame.h>
#include <array>

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
    std::array<std::uint8_t, varint_traits<std::uint32_t>::max> vi;

    if (in_size > UINT32_MAX)
        do_throw("lz4f compress: invalid size");

    auto const original_size_bytes = write_varint(vi.data(), in_size);

    std::size_t const out_capacity = LZ4F_compressFrameBound(in_size, NULL);

    auto* compressed = reinterpret_cast<std::uint8_t*>(
            bf(original_size_bytes + out_capacity));
    result.first = compressed;

    std::memcpy(compressed, vi.data(), original_size_bytes);

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

template<typename InputStream>
bool
copy_stream(InputStream& in, std::uint8_t* dst, int size, std::vector<int> &used_size)
{
    const void * data = NULL;
    int data_size = 0;
    int copied = 0;

    while (copied != size && in.Next(&data, &data_size))
    {
        auto sz = data_size >= (size - copied) ? (size - copied) : data_size;
        std::memcpy(dst + copied, data, sz);
        copied += sz;
        used_size.push_back(data_size);
    }

    return copied == size;
}

template<typename InputStream>
std::size_t
get_original_size(InputStream& in)
{
    std::array<std::uint8_t, varint_traits<std::uint32_t>::max> vi;
    std::size_t original_size = 0;
    const void * data = NULL;
    int data_size = 0;
    std::vector<int> used_size;

    if (!in.Next(&data, &data_size))
        do_throw("lz4f decompress: invalid input size");

    used_size.push_back(data_size);

    const void *p = data;

    if (data_size < varint_traits<std::uint32_t>::max)
    {
        std::memcpy(vi.data(), data, data_size);
        if (!copy_stream(in, vi.data() + data_size,
                varint_traits<std::uint32_t>::max - data_size, used_size))
            do_throw("lz4f decompress: header");
        p = vi.data();
    }

    auto original_size_bytes = read_varint(p, varint_traits<std::uint32_t>::max, original_size);

    if (original_size_bytes == 0)
        do_throw("lz4f decompress:: original_size_bytes == 0");

    // rewind the stream
    for (auto it = used_size.rbegin(); it != used_size.rend(); ++it)
        in.BackUp(*it);
    in.Skip(original_size_bytes);

    return original_size;
}

template<typename InputStream, typename BufferFactory>
std::pair<void const*, std::size_t>
lz4f_decompress(InputStream &in,
                std::size_t in_size, BufferFactory&& bf)
{
    std::pair<void const*, std::size_t> result{0, 0};
    LZ4F_dctx_s* dctx;
    const void * compressed_chunk = NULL;
    int chunk_size = 0;
    std::size_t decompressed_size = 0;
    std::uint8_t * decompressed = NULL;
    const void *compressed = NULL;
    bool done = false;
    std::vector<std::uint8_t> buffer;

    std::size_t const dctx_status = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(dctx_status))
        do_throw("lz4f decompress: failed decompression context");

    result.second = get_original_size(in);
    decompressed = bf(result.second);
    result.first = decompressed;

    while (!done && in.Next(&compressed_chunk, &chunk_size))
    {
        if (chunk_size == 0)
            continue;

        if (buffer.size() != 0)
        {
            auto sz = buffer.size();
            chunk_size = ((chunk_size + sz) <= in_size) ? chunk_size : (in_size - sz);
            buffer.resize(sz + chunk_size);
            std::memcpy(buffer.data() + sz, compressed_chunk, chunk_size);
            compressed = buffer.data();
            chunk_size += sz;
        }
        else
        {
            compressed = compressed_chunk;
            chunk_size = (chunk_size <= in_size) ? chunk_size : in_size;
        }

        std::size_t dst_size = result.second - decompressed_size;
        std::size_t src_size = chunk_size;
        size_t size = LZ4F_decompress(dctx,
                                      decompressed + decompressed_size,
                                      &dst_size,
                                      compressed,
                                      &src_size,
                                      NULL);
        if (LZ4F_isError(size))
            do_throw("lz4f decompress: failed");

        decompressed_size += dst_size;
        in_size -= src_size;

        if (src_size != chunk_size)
        {
            std::memmove(buffer.data(),
                         reinterpret_cast<uint8_t const*>(compressed) + src_size,
                      chunk_size - src_size);
            buffer.resize(chunk_size - src_size);
        }
        else if (buffer.size() > 0)
            buffer.resize(0);

        done = decompressed_size == result.second;
    }

    return result;
}

} // compression

} // ripple

#endif //RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED
