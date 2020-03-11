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

#ifndef RIPPLE_OVERLAY_MESSAGE_H_INCLUDED
#define RIPPLE_OVERLAY_MESSAGE_H_INCLUDED

#include <ripple/overlay/Compression.h>
#include <ripple/protocol/messages.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <memory>
#include <type_traits>

namespace ripple {

// VFALCO NOTE If we forward declare Message and write out shared_ptr
//             instead of using the in-class type alias, we can remove the entire
//             ripple.pb.h from the main headers.
//

// packaging of messages into length/type-prepended buffers
// ready for transmission.
//
// Message implements simple "packing" of protocol buffers Messages into
// a string prepended by a header specifying the message length.
// MessageType should be a Message class generated by the protobuf compiler.
//

using namespace compression;

class Message : public std::enable_shared_from_this <Message>
{
public:
    /** Constructor
     * @param message Protocol message to serialize
     * @param type Protocol message type
     */
    Message (::google::protobuf::Message const& message, int type);

public:
    /** Retrieve the packed message data.
     * @param compressed Request compressed (Compress::On) or uncompressed (Compress::Off) payload buffer
     * @return Payload buffer
     */
    std::vector <uint8_t> const&
    getBuffer (Compressed compressed)
    {
        if (compressed == Compressed::Off)
            return mBuffer;

        if (!mCompressedRequested)
            compress(mType);

        if (mBufferCompressed.size() > 0)
            return mBufferCompressed;
        else
            return mBuffer;
    }

    /** Get the traffic category */
    std::size_t
    getCategory () const
    {
        return mCategory;
    }

private:
    std::vector <uint8_t> mBuffer;
    std::vector <uint8_t> mBufferCompressed;
    std::size_t mCategory;
    bool mCompressedRequested;
    int mType;

    /** Set the payload header
     * @param in Pointer to the payload
     * @param messageBytes Size of the payload minus the header size
     * @param type Protocol message type
     * @param compressed Compression flag - Compressed::On if the payload is compressed
     * @param comprAlgorithm Compression algorithm used in compression, currently LZ4 only
     */
    void setHeader(std::uint8_t *in, uint32_t messageBytes, int type,
            Compressed compressed,
            std::uint8_t comprAlgorithm = ripple::compression::Algorithm::LZ4);
    /** Try to compress the payload. */
    void compress(int type);
};

}

#endif
