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

#include <ripple/basics/contract.h>
#include <ripple/basics/varint_common.h>
#include <lz4frame.h>
#include <array>
#include <cstring>

namespace ripple {

namespace compression_algorithms {

/** Convenience wrapper for Throw
 * @param message Message to log/throw
 */
inline void doThrow(const char *message)
{
    Throw<std::runtime_error>(message);
}

/** LZ4 frame compression. Compressed data is prefixed with varint header containing the size of the original data.
 * @tparam BufferFactory Callable object or lambda.
 *     Takes the requested buffer size and returns allocated buffer pointer.
 * @param in Data to compress
 * @param inSize Size of the data
 * @param bf Compressed buffer allocator
 * @return Size of compressed data, or zero if failed to compress
 */
template<typename BufferFactory>
std::size_t
lz4fCompress(void const* in,
             std::size_t inSize, BufferFactory&& bf)
{
    std::array<std::uint8_t, varint_traits<std::uint32_t>::max> vi;

    if (inSize > UINT32_MAX)
        doThrow("lz4f compress: invalid size");

    auto const originalSizeBytes = write_varint(vi.data(), inSize);

    std::size_t const outCapacity = LZ4F_compressFrameBound(inSize, nullptr);

    // Request the caller to allocate and return the buffer to hold compressed data
    auto compressed = bf(originalSizeBytes + outCapacity);

    std::memcpy(compressed, vi.data(), originalSizeBytes);

    size_t compressedSize = LZ4F_compressFrame(
            compressed + originalSizeBytes,
            outCapacity,
            in,
            inSize,
            nullptr);
    if (LZ4F_isError(compressedSize))
        doThrow("lz4f failed compress update");

    return originalSizeBytes + compressedSize;
}

/** Copy data from the input stream if the payload header containing
 * the size of the uncompressed data is split between multiple chunks.
 * @tparam InputStream ZeroCopyInputStream
 * @param in Source input stream
 * @param dst Destination buffer
 * @param size Destination size
 * @param usedSize Vector of consumed input chunk sizes
 * @return true if the stream has enough data to copy
 */
template<typename InputStream>
bool
copyStream(InputStream& in, std::uint8_t* dst, int size, std::vector<int> &usedSize)
{
    const void* data = nullptr;
    int dataSize = 0;
    int copied = 0;

    while (copied != size && in.Next(&data, &dataSize))
    {
        auto sz = dataSize >= (size - copied) ? (size - copied) : dataSize;
        std::memcpy(dst + copied, data, sz);
        copied += sz;
        usedSize.push_back(dataSize);
    }

    return copied == size;
}

/** Get the size of uncompressed data from the varint header.
 * @tparam InputStream ZeroCopyInputStream
 * @param in Source input stream
 * @return Size of the uncompressed data
 */
template<typename InputStream>
std::size_t
getOriginalSize(InputStream& in)
{
    std::array<std::uint8_t, varint_traits<std::uint32_t>::max> vi;
    std::size_t originalSize = 0;
    const void* data = nullptr;
    int dataSize = 0;
    std::vector<int> usedSize;

    if (!in.Next(&data, &dataSize))
        doThrow("lz4f decompress: invalid input size");

    usedSize.push_back(dataSize);

    auto p = data;

    if (dataSize < varint_traits<std::uint32_t>::max)
    {
        std::memcpy(vi.data(), data, dataSize);
        if (!copyStream(in, vi.data() + dataSize,
                        varint_traits<std::uint32_t>::max - dataSize, usedSize))
            doThrow("lz4f decompress: header");
        p = vi.data();
    }

    auto originalSizeBytes = read_varint(p, varint_traits<std::uint32_t>::max, originalSize);

    if (originalSizeBytes == 0)
        doThrow("lz4f decompress:: originalSizeBytes == 0");

    // Rewind the stream
    for (auto it = usedSize.rbegin(); it != usedSize.rend(); ++it)
        in.BackUp(*it);
    in.Skip(originalSizeBytes);

    return originalSize;
}

/** Read next available compressed chunk. If the buffer of cached compressed data is not empty then the chunk
 * is copied into the buffer.
 * @tparam InputStream ZeroCopyInputStream
 * @param in Source input stream
 * @param inSize Remaining size of the compressed data to read from the input stream.
 * @param compressedChunk Pointer to the next chunk
 * @param chunkSize Size of the next chunk
 * @param buffer Cached compressed data
 * @return true if the stream is not empty
 */
template<typename InputStream>
bool
nextChunk(InputStream& in, std::size_t inSize,
          void const*& compressedChunk, int &chunkSize,
          std::vector<std::uint8_t> &buffer)
{
    bool res = in.Next(&compressedChunk, &chunkSize);

    if (!res && buffer.empty())
        return false;

    /* There is existing data in the buffer that still needs to be processed.
     * Copy the new data to the end of the buffer
     */
    if (!buffer.empty())
    {
        auto sz = buffer.size();
        chunkSize = ((chunkSize + sz) <= inSize) ? chunkSize : (inSize - sz);
        buffer.resize(sz + chunkSize);
        std::memcpy(buffer.data() + sz, compressedChunk, chunkSize);
        compressedChunk = buffer.data();
        chunkSize = buffer.size();
    }
    else
        chunkSize = (chunkSize <= inSize) ? chunkSize : inSize;

    return true;
}

/** Update cached compressed data buffer.
 * Handles the case when decompress does not use all input bytes.
 * @param srcSize Input bytes consumed by the decompress function
 * @param compressedChunk Pointer to compressed data
 * @param chunkSize Size of compressed data
 * @param buffer Compressed data cache
 */
inline
void
updateBuffer(std::size_t srcSize,
             const void* compressedChunk, int chunkSize, std::vector<std::uint8_t> &buffer)
{
    if (srcSize != chunkSize)
    {
        auto p = reinterpret_cast<uint8_t const *>(compressedChunk) + srcSize;
        auto s = chunkSize - srcSize;
        if (!buffer.empty())
        {
            // If the cached data is not empty then the unused bytes are
            // already in the cache - have to move
            std::memmove(buffer.data(), p, s);
            buffer.resize(s);
        }
        else
        {
            // If the cached data is empty then copy unused bytes from
            // the chunk
            buffer.resize(s);
            std::memcpy(buffer.data(), p, s);
        }
    }
    else
        buffer.clear();
}

/** LZ4 frame decompression. Read compressed data from ZeroCopyInputStream. The data must be prefixed
 * with varint header containing the size of uncompressed data.
 * @tparam InputStream ZeroCopyInputStream
 * @tparam BufferFactory Callable object or lambda.
 *    Takes the requested buffer size and returns allocated buffer pointer.
 * @param in Input source stream
 * @param inSize Size of compressed data
 * @param bf Decompressed buffer allocator
 * @return Size of decompressed data or zero if failed to decompress
 */
template<typename InputStream, typename BufferFactory>
std::size_t
lz4fDecompress(InputStream& in,
               std::size_t inSize, BufferFactory&& bf)
{
    LZ4F_dctx_s* dctx;
    void const* compressedChunk = nullptr;
    int chunkSize = 0;
    std::uint8_t* decompressed = nullptr;
    std::size_t decompressedSize = 0;
    std::vector<std::uint8_t> buffer;

    if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)))
        doThrow("lz4f decompress: failed decompression context");

    auto originalSize = getOriginalSize(in);
    // Request the caller to allocate and return the buffer to hold decompressed data
    decompressed = bf(originalSize);

    // Read the next available chunk from the input stream or the buffer. If decompress function does not use
    // all input bytes then the unprocessed bytes are copied into the buffer (updateBuffer) and the next chunk
    // is appended to the buffer (nextChunk).
    while (decompressedSize != originalSize &&
           nextChunk(in, inSize, compressedChunk, chunkSize, buffer))
    {
        std::size_t dstSize = originalSize - decompressedSize;
        std::size_t srcSize = chunkSize;
        size_t size = LZ4F_decompress(dctx,
                                      decompressed + decompressedSize,
                                      &dstSize,
                                      compressedChunk,
                                      &srcSize,
                                      nullptr);
        if (LZ4F_isError(size) || srcSize == 0)
            doThrow("lz4f decompress: failed");

        decompressedSize += dstSize;
        inSize -= srcSize;

        updateBuffer(srcSize, compressedChunk, chunkSize, buffer);
    }

    if (LZ4F_isError(LZ4F_freeDecompressionContext(dctx)))
        doThrow("lz4 decompress: failed free decompression context");

    if (decompressedSize != originalSize)
        doThrow("lz4 decompress: insufficient input data");

    return originalSize;
}

} // compression

} // ripple

#endif //RIPPLED_COMPRESSIONALGORITHMS_H_INCLUDED
