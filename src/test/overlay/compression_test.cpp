//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright 2020 Ripple Labs Inc.

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

#include <ripple/beast/unit_test.h>
#include <ripple.pb.h>
#include <ripple/overlay/Compression.h>
#include <ripple/app/misc/Manifest.h>
#include <ripple/protocol/PublicKey.h>
#include <ripple/protocol/SecretKey.h>
#include <ripple/protocol/HashPrefix.h>
#include <ripple/protocol/Sign.h>
#include <ripple/overlay/Message.h>
#include <ripple/overlay/Compression.h>
#include <ripple/overlay/impl/ZeroCopyStream.h>
#include <ripple/overlay/impl/ProtocolMessage.h>
#include <boost/beast/core/multi_buffer.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/asio/ip/address_v4.hpp>

namespace ripple {

class compression_test : public beast::unit_test::suite
{

public:
    compression_test () {}

    template<typename T>
    void
    do_test (std::shared_ptr<T> proto, protocol::MessageType mt, const char *msg) {

        printf ("=== compress/decompress %s ===\n", msg);
        Message m(*proto, mt, true);

        auto &buffer = m.getBuffer(true);

        printf ("==> compressed, original %d bytes, compressed %d bytes\n", m.getBuffer(false).size(),
                m.getBuffer(true).size());

        std::vector<std::uint8_t> decompressed;
        boost::beast::multi_buffer buffers;
        uint8_t n = 4;
        // simulate multi-buffer
        auto sz = buffer.size() / n;
        for (int i = 0; i < n; i++) {
            auto start = buffer.begin() + sz * i;
            auto end = i < n - 1 ? (buffer.begin() + sz * (i + 1)) : buffer.end();
            std::vector<std::uint8_t> slice(start, end);
            buffers.commit(
                    boost::asio::buffer_copy(buffers.prepare(slice.size()), boost::asio::buffer(slice)));
        }
        auto header = detail::parseMessageHeader(buffers.data(), buffer.size());

        printf ("==> parsed header: buffers size %d, compressed %d, algorithm %d, header size %d, payload size %d\n",
                buffers.size(), header->compressed, header->algorithm, header->header_size, header->payload_wire_size);

        BEAST_EXPECT(header->payload_wire_size == buffer.size() - header->header_size);

        ZeroCopyInputStream stream(buffers.data());
        stream.Skip(header->header_size);

        auto res = ripple::compression::decompress(stream, header->payload_wire_size,
                                                   [this, &decompressed](size_t size) {
            printf ("==> decompress requested %d bytes\n", size);
            decompressed.resize(size);
            return decompressed.data();
        });
        auto const proto1 = std::make_shared<T>();

        BEAST_EXPECT(proto1->ParseFromArray(std::get<0>(res), std::get<1>(res)));
        std::string str = proto->SerializeAsString();
        std::string str1 = proto1->SerializeAsString();
        BEAST_EXPECT(str == str1);
        printf ("\n");
    }

    std::shared_ptr<protocol::TMManifests>
    buildManifests(int n)
    {
        auto manifests = std::make_shared<protocol::TMManifests>();
        manifests->mutable_list()->Reserve(n);
        for (int i = 0; i < n; i++) {
            auto master = randomKeyPair(KeyType::ed25519);
            auto signing = randomKeyPair(KeyType::ed25519);
            STObject st(sfGeneric);
            st[sfSequence] = i;
            st[sfPublicKey] = std::get<0>(master);
            st[sfSigningPubKey] = std::get<0>(signing);
            st[sfDomain] = makeSlice(std::string("example") + std::to_string(i) + std::string(".com"));
            sign(st, HashPrefix::manifest, KeyType::ed25519, std::get<1>(master), sfMasterSignature);
            sign(st, HashPrefix::manifest, KeyType::ed25519, std::get<1>(signing));
            Serializer s;
            st.add(s);
            manifests->add_list()->set_stobject(s.data(), s.size());
        }
        return manifests;
    }

    std::shared_ptr<protocol::TMEndpoints>
    buildEndpoints(int n)
    {
        auto endpoints = std::make_shared<protocol::TMEndpoints>();
        endpoints->mutable_endpoints()->Reserve(n);
        for (int i = 0; i < n; i++) {
            auto *endpoint = endpoints->add_endpoints();
            endpoint->set_hops(i);
            std::string addr = std::string("10.0.1.") + std::to_string(i);
            endpoint->mutable_ipv4()->set_ipv4(
                    boost::endian::native_to_big(boost::asio::ip::address_v4::from_string(addr).to_uint()));
            endpoint->mutable_ipv4()->set_ipv4port(i);
        }
        endpoints->set_version(2);

        return endpoints;
    }

    void
    testProtocol ()
    {
        testcase ("Message Compression");
        protocol::TMManifests manifests;
        protocol::TMEndpoints endpoints;
        protocol::TMTransaction transaction;
        protocol::TMGetLedger get_ledger;
        protocol::TMLedgerData ledger_data;
        protocol::TMGetObjectByHash get_object;
        protocol::TMValidatorList validator_list;

        do_test(buildManifests(20), protocol::mtMANIFESTS, "TMManifests");
        do_test(buildEndpoints(10), protocol::mtENDPOINTS, "TMEndpoints");
    }

    void run () override
    {
        testProtocol ();
    }

};

BEAST_DEFINE_TESTSUITE_MANUAL_PRIO(compression,ripple_data,ripple,20);

}