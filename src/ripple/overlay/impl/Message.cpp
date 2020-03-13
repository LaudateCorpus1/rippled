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

#include <ripple/basics/safe_cast.h>
#include <ripple/overlay/Message.h>
#include <ripple/overlay/impl/TrafficCount.h>
#include <cstdint>

namespace ripple {

/** Number of bytes in a message header. */
std::size_t constexpr headerBytes = 6;

Message::Message (::google::protobuf::Message const& message, int type)
    : category_(TrafficCount::categorize(message, type, false))
    , alreadyRequested_(false)
{

#if defined(GOOGLE_PROTOBUF_VERSION) && (GOOGLE_PROTOBUF_VERSION >= 3011000)
    auto const messageBytes = message.ByteSizeLong ();
#else
    unsigned const messageBytes = message.ByteSize ();
#endif

    assert (messageBytes != 0);

    buffer_.resize (headerBytes + messageBytes);

    setHeader(buffer_.data(), messageBytes, type, Compressed::Off);

    if (messageBytes != 0)
        message.SerializeToArray(buffer_.data() + headerBytes, messageBytes);
}

void
Message::compress()
{
    std::lock_guard<std::mutex> lock(mutex_);

    // 'redundant' check because multiple threads may call compress().
    // But once the message is already compressed or tried being compressed
    // getBuffer() call  will not call compress ()
    if (alreadyRequested_)
        return;

    auto const messageBytes = buffer_.size () - headerBytes;

    auto type = getType(buffer_.data());

    bool const compressible = [&]{
        if (messageBytes <= 70)
            return false;
        switch(type)
        {
            case protocol::mtMANIFESTS:
            case protocol::mtENDPOINTS:
            case protocol::mtTRANSACTION:
            case protocol::mtGET_LEDGER:
            case protocol::mtLEDGER_DATA:
            case protocol::mtGET_OBJECTS:
            case protocol::mtVALIDATORLIST:
                return true;
            case protocol::mtPING:
            case protocol::mtCLUSTER:
            case protocol::mtPROPOSE_LEDGER:
            case protocol::mtSTATUS_CHANGE:
            case protocol::mtHAVE_SET:
            case protocol::mtVALIDATION:
            case protocol::mtGET_SHARD_INFO:
            case protocol::mtSHARD_INFO:
            case protocol::mtGET_PEER_SHARD_INFO:
            case protocol::mtPEER_SHARD_INFO:
                break;
        }
        return false;
    }();

    if (compressible)
    {
        auto *payload = static_cast<void const*>(buffer_.data() + headerBytes);

        auto compressedSize = ripple::compression::compress(
                payload,
                messageBytes,
                [&](std::size_t in_size) { // size of required compressed buffer
                    bufferCompressed_.resize(in_size + headerBytes);
                    return (bufferCompressed_.data() + headerBytes);
                });

        if (compressedSize < messageBytes)
        {
            bufferCompressed_.resize(headerBytes + compressedSize);
            setHeader(bufferCompressed_.data(), compressedSize, type, Compressed::On);
        }
        else
            bufferCompressed_.resize(0);
    }

    alreadyRequested_ = true;
}

/** Set payload header
 * 47       Set to 1, indicates the message is compressed
 * 46-44    Compression algorithm, value 1-7. Set to 1 to indicate LZ4 compression
 * 43-42    Set to 0
 * 41-16    Payload size
 * 15-0	    Message Type
*/
void
Message::setHeader(std::uint8_t *in, uint32_t messageBytes, int type,
                   Compressed compressed, std::uint8_t comprAlgorithm)
{
    uint8_t compression = (compressed == Compressed::On?0xF0:0) & (0x80 | (comprAlgorithm << 4));

    *in++ = static_cast<std::uint8_t>(((messageBytes >> 24) | compression) & 0xFF);
    *in++ = static_cast<std::uint8_t>((messageBytes >> 16) & 0xFF);
    *in++ = static_cast<std::uint8_t>((messageBytes >> 8) & 0xFF);
    *in++ = static_cast<std::uint8_t>(messageBytes & 0xFF);

    *in++ = static_cast<std::uint8_t>((type >> 8) & 0xFF);
    *in = static_cast<std::uint8_t> (type & 0xFF);
}

}
