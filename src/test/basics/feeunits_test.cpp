//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2019 Ripple Labs Inc.

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

#include <ripple/basics/feeunits.h>
#include <ripple/beast/unit_test.h>
#include <ripple/protocol/SystemParameters.h>
#include <type_traits>

namespace ripple {
namespace test {

class feeunits_test
    : public beast::unit_test::suite
{
private:

public:
    void run() override
    {
        BEAST_EXPECT(INITIAL_XRP.drops() == 100'000'000'000'000'000);
        BEAST_EXPECT(INITIAL_XRP ==
            XRPAmount{ 100'000'000'000'000'000 });
        {
            XRPAmount x{ 100 };
            BEAST_EXPECT(x.drops() == 100);
            BEAST_EXPECT((std::is_same_v<decltype(x)::unit_type,
                feeunit::drop_tag>));
            auto y = 4u * x;
            BEAST_EXPECT(y.value() == 400);
            BEAST_EXPECT((std::is_same_v<decltype(y)::unit_type,
                feeunit::drop_tag>));

            auto z = 4 * y;
            BEAST_EXPECT(z.value() == 1600);
            BEAST_EXPECT((std::is_same_v<decltype(z)::unit_type,
                feeunit::drop_tag>));

            FeeUnit32 f{ 10 };
            FeeUnit32 baseFee{ 100 };

            auto drops = mulDiv(baseFee, x, f).second;

            BEAST_EXPECT(drops.value() == 1000);
            BEAST_EXPECT((std::is_same_v<decltype(drops)::unit_type,
                feeunit::drop_tag>));
            BEAST_EXPECT((std::is_same_v<decltype(drops), XRPAmount>));
        }
        {
            XRPAmount x{ 100 };
            BEAST_EXPECT(x.value() == 100);
            BEAST_EXPECT((std::is_same_v<decltype(x)::unit_type,
                feeunit::drop_tag>));
            auto y = 4u * x;
            BEAST_EXPECT(y.value() == 400);
            BEAST_EXPECT((std::is_same_v<decltype(y)::unit_type,
                feeunit::drop_tag>));

            FeeUnit64 f{ 10 };
            FeeUnit64 baseFee{ 100 };

            auto drops = mulDiv(baseFee, x, f).second;

            BEAST_EXPECT(drops.value() == 1000);
            BEAST_EXPECT((std::is_same_v<decltype(drops)::unit_type,
                feeunit::drop_tag>));
            BEAST_EXPECT((std::is_same_v<decltype(drops), XRPAmount>));
        }
        {
            FeeLevel64 x{ 1024 };
            BEAST_EXPECT(x.value() == 1024);
            BEAST_EXPECT((std::is_same_v<decltype(x)::unit_type,
                feeunit::feelevel_tag>));
            std::uint64_t m = 4;
            auto y = m * x;
            BEAST_EXPECT(y.value() == 4096);
            BEAST_EXPECT((std::is_same_v<decltype(y)::unit_type,
                feeunit::feelevel_tag>));

            XRPAmount basefee{ 10 };
            FeeLevel64 referencefee{ 256 };

            auto drops = mulDiv(x, basefee, referencefee).second;

            BEAST_EXPECT(drops.value() == 40);
            BEAST_EXPECT((std::is_same_v<decltype(drops)::unit_type,
                feeunit::drop_tag>));
            BEAST_EXPECT((std::is_same_v<decltype(drops), XRPAmount>));
        }

        // Json value functionality
        {
            FeeUnit32 x{std::numeric_limits<std::uint32_t>::max()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::uintValue);
            BEAST_EXPECT(y == Json::Value{x.fee()});
        }

        {
            FeeUnit32 x{std::numeric_limits<std::uint32_t>::min()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::uintValue);
            BEAST_EXPECT(y == Json::Value{x.fee()});
        }

        {
            FeeUnit64 x{std::numeric_limits<std::uint64_t>::max()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::uintValue);
            BEAST_EXPECT(y ==
                Json::Value{std::numeric_limits<std::uint32_t>::max()});
        }

        {
            FeeUnit64 x{std::numeric_limits<std::uint64_t>::min()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::uintValue);
            BEAST_EXPECT(y == Json::Value{x.fee()});
        }

        {
            FeeLevelDouble x{std::numeric_limits<double>::max()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::realValue);
            BEAST_EXPECT(y == Json::Value{std::numeric_limits<double>::max()});
        }

        {
            FeeLevelDouble x{std::numeric_limits<double>::min()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::realValue);
            BEAST_EXPECT(y == Json::Value{std::numeric_limits<double>::min()});
        }

        {
            XRPAmount x{std::numeric_limits<std::int64_t>::max()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::intValue);
            BEAST_EXPECT(y ==
                Json::Value{std::numeric_limits<std::int32_t>::max()});
        }

        {
            XRPAmount x{std::numeric_limits<std::int64_t>::min()};
            auto y = x.json();
            BEAST_EXPECT(y.type() == Json::intValue);
            BEAST_EXPECT(y ==
                Json::Value{std::numeric_limits<std::int32_t>::min()});
        }
    }
};

BEAST_DEFINE_TESTSUITE(feeunits,ripple_basics,ripple);

} // test
} //ripple
