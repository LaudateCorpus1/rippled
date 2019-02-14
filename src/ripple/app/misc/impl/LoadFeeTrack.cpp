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

#include <ripple/app/misc/LoadFeeTrack.h>
#include <ripple/basics/contract.h>
#include <ripple/basics/feeunits.h>
#include <ripple/basics/Log.h>
#include <ripple/basics/safe_cast.h>
#include <ripple/core/Config.h>
#include <ripple/ledger/ReadView.h>
#include <ripple/protocol/jss.h>
#include <ripple/protocol/STAmount.h>

#include <cstdint>
#include <numeric>
#include <type_traits>

namespace ripple {

bool
LoadFeeTrack::raiseLocalFee ()
{
    std::lock_guard sl (lock_);

    if (++raiseCount_ < 2)
        return false;

    std::uint32_t origFee = localTxnLoadFee_;

    // make sure this fee takes effect
    if (localTxnLoadFee_ < remoteTxnLoadFee_)
        localTxnLoadFee_ = remoteTxnLoadFee_;

    // Increase slowly
    localTxnLoadFee_ += (localTxnLoadFee_ / lftFeeIncFraction);

    if (localTxnLoadFee_ > lftFeeMax)
        localTxnLoadFee_ = lftFeeMax;

    if (origFee == localTxnLoadFee_)
        return false;

    JLOG(j_.debug()) << "Local load fee raised from " <<
        origFee << " to " << localTxnLoadFee_;
    return true;
}

bool
LoadFeeTrack::lowerLocalFee ()
{
    std::lock_guard sl (lock_);
    std::uint32_t origFee = localTxnLoadFee_;
    raiseCount_ = 0;

    // Reduce slowly
    localTxnLoadFee_ -= (localTxnLoadFee_ / lftFeeDecFraction );

    if (localTxnLoadFee_ < lftNormalFee)
        localTxnLoadFee_ = lftNormalFee;

    if (origFee == localTxnLoadFee_)
        return false;

    JLOG(j_.debug()) << "Local load fee lowered from " <<
        origFee << " to " << localTxnLoadFee_;
    return true;
}

//------------------------------------------------------------------------------

namespace detail
{

struct xrp_unit_product_tag;

using xrp_unit_product =
    units::TaggedFee<detail::xrp_unit_product_tag, std::uint64_t>;

} // detail

detail::xrp_unit_product
operator* (FeeUnit64 lhs, XRPAmount rhs)
{
    return detail::xrp_unit_product{ lhs.fee() * rhs.drops() };
}

XRPAmount
operator/ (detail::xrp_unit_product lhs, FeeUnit64 rhs)
{
    return{ lhs.fee() / rhs.fee() };
}


// Scale using load as well as base rate
XRPAmount
scaleFeeLoad(FeeUnit64 fee, LoadFeeTrack const& feeTrack,
    Fees const& fees, bool bUnlimited)
{
    if (fee == 0)
        return 0;

    auto lowestTerms1 = [](auto& a, auto& b)
    {
        if (auto const g = std::gcd(a, b.value()))
        {
            a /= g;
            b /= g;
        }
    };

    // Normally, types with different units wouldn't be mathematically
    // compatible. This function is an exception.
    auto lowestTerms2 = [](auto& a, auto& b)
    {
        if (auto const g = std::gcd(a.value(), b.value()))
        {
            a /= g;
            b /= g;
        }
    };

    // Normally, these types wouldn't be swappable. This function is an
    // exception
    auto maybe_swap = [](auto& lhs, auto& rhs)
    {
        if (lhs.value() < rhs.value())
        {
            auto tmp = lhs.value();
            lhs = rhs.value();
            rhs = tmp;
        }
        // double check
        assert(lhs.value() >= rhs.value());
    };

    // Collect the fee rates
    auto [feeFactor, uRemFee] = feeTrack.getScalingFactors();

    // Let privileged users pay the normal fee until
    //   the local load exceeds four times the remote.
    if (bUnlimited && (feeFactor > uRemFee) && (feeFactor < (4 * uRemFee)))
        feeFactor = uRemFee;

    auto baseFee = fees.base;
    // Compute:
    // fee = fee * baseFee * feeFactor / (fees.units * lftNormalFee);
    // without overflow, and as accurately as possible

    // The denominator of the fraction we're trying to compute.
    // fees.units and lftNormalFee are both 32 bit,
    //  so the multiplication can't overflow.
    auto den = FeeUnit64{ fees.units }
        * safe_cast<std::uint64_t>(feeTrack.getLoadBase());
    // Reduce fee * baseFee * feeFactor / (fees.units * lftNormalFee)
    // to lowest terms.
    lowestTerms2(fee, den);
    lowestTerms2(baseFee, den);
    lowestTerms1(feeFactor, den);

    // fee and baseFee are 64 bit, feeFactor is 32 bit
    // Order fee and baseFee largest first
    maybe_swap(fee, baseFee);
    // If baseFee * feeFactor overflows, the final result will overflow
    constexpr XRPAmount max =
        std::numeric_limits<XRPAmount::value_type>::max();
    if (baseFee > max / feeFactor)
        Throw<std::overflow_error> ("scaleFeeLoad");
    baseFee *= feeFactor;
    // Reorder fee and baseFee
    maybe_swap(fee, baseFee);
    // If fee * baseFee overflows, do the division first
    if (fee > FeeUnit64{ max / baseFee })
    {
        // Do the division first, on the larger of fee and baseFee
        auto const factor = fee / den;
        // If factor * basefee ( == fee / den * baseFee ) will overflow,
        //  throw
        if (factor > max / baseFee)
            Throw<std::overflow_error> ("scaleFeeLoad");
        return factor * baseFee;
    }
    else
    {
        // Otherwise fee * baseFee won't overflow,
        //   so do it prior to the division.
        auto const product = fee * baseFee;
        return product / den;
    }
}

} // ripple
