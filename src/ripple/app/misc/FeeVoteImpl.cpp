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

#include <ripple/protocol/st.h>
#include <ripple/app/misc/FeeVote.h>
#include <ripple/app/main/Application.h>
#include <ripple/protocol/STValidation.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/beast/utility/Journal.h>

namespace ripple {

namespace detail {

template<class T>
class VotableValue
{
private:
    using value_type = T;
    value_type const mCurrent;   // The current setting
    value_type const mTarget;    // The setting we want
    std::map <value_type, int> mVoteMap;

public:
    VotableValue (value_type current, value_type target)
        : mCurrent (current)
        , mTarget (target)
    {
        // Add our vote
        ++mVoteMap[mTarget];
    }

    void
    addVote(value_type vote)
    {
        ++mVoteMap[vote];
    }

    void
    noVote()
    {
        addVote (mCurrent);
    }

    value_type
    getVotes() const;
};

template<class T>
auto
VotableValue<T>::getVotes() const
    -> value_type
{
    value_type ourVote = mCurrent;
    int weight = 0;
    for (auto const& [key, val] : mVoteMap)
    {
        // Take most voted value between current and target, inclusive
        if ((key <= std::max (mTarget, mCurrent)) &&
                (key >= std::min (mTarget, mCurrent)) &&
                (val > weight))
        {
            ourVote = key;
            weight = val;
        }
    }

    return ourVote;
}

}

//------------------------------------------------------------------------------

class FeeVoteImpl : public FeeVote
{
private:
    Setup target_;
    beast::Journal journal_;

public:
    FeeVoteImpl (Setup const& setup, beast::Journal journal);

    void
    doValidation (std::shared_ptr<ReadView const> const& lastClosedLedger,
        STValidation::FeeSettings& fees) override;

    void
    doVoting (std::shared_ptr<ReadView const> const& lastClosedLedger,
        std::vector<STValidation::pointer> const& parentValidations,
        std::shared_ptr<SHAMap> const& initialPosition) override;
};

//--------------------------------------------------------------------------

FeeVoteImpl::FeeVoteImpl (Setup const& setup, beast::Journal journal)
    : target_ (setup)
    , journal_ (journal)
{
}

void
FeeVoteImpl::doValidation(
    std::shared_ptr<ReadView const> const& lastClosedLedger,
        STValidation::FeeSettings& fees)
{
    if (lastClosedLedger->fees().base != target_.reference_fee)
    {
        JLOG(journal_.info()) <<
            "Voting for base fee of " << target_.reference_fee;

        fees.baseFee = target_.reference_fee;
    }

    if (lastClosedLedger->fees().accountReserve(0) != target_.account_reserve)
    {
        JLOG(journal_.info()) <<
            "Voting for base reserve of " << target_.account_reserve;

        fees.reserveBase = target_.account_reserve;
    }

    if (lastClosedLedger->fees().increment != target_.owner_reserve)
    {
        JLOG(journal_.info()) <<
            "Voting for reserve increment of " << target_.owner_reserve;

        fees.reserveIncrement = target_.owner_reserve;
    }
}

void
FeeVoteImpl::doVoting(
    std::shared_ptr<ReadView const> const& lastClosedLedger,
    std::vector<STValidation::pointer> const& set,
    std::shared_ptr<SHAMap> const& initialPosition)
{
    // LCL must be flag ledger
    assert ((lastClosedLedger->info().seq % 256) == 0);

    detail::VotableValue<XRPAmountU64> baseFeeVote (
        lastClosedLedger->fees().base.as<XRPAmountU64>(),
        target_.reference_fee);

    detail::VotableValue<XRPAmountU32> baseReserveVote(
        lastClosedLedger->fees().accountReserve(0).as<XRPAmountU32>(),
        target_.account_reserve);

    detail::VotableValue<XRPAmountU32> incReserveVote (
        lastClosedLedger->fees().increment.as<XRPAmountU32>(),
        target_.owner_reserve);

    for (auto const& val : set)
    {
        if (val->isTrusted ())
        {
            if (val->isFieldPresent (sfBaseFee))
            {
                baseFeeVote.addVote (val->getFieldU64 (sfBaseFee));
            }
            else
            {
                baseFeeVote.noVote ();
            }

            if (val->isFieldPresent (sfReserveBase))
            {
                baseReserveVote.addVote (val->getFieldU32 (sfReserveBase));
            }
            else
            {
                baseReserveVote.noVote ();
            }

            if (val->isFieldPresent (sfReserveIncrement))
            {
                incReserveVote.addVote (val->getFieldU32 (sfReserveIncrement));
            }
            else
            {
                incReserveVote.noVote ();
            }
        }
    }

    // choose our positions
    auto const baseFee = baseFeeVote.getVotes ();
    auto const baseReserve = baseReserveVote.getVotes ();
    auto const incReserve = incReserveVote.getVotes ();
    constexpr FeeUnit32 feeUnits = Setup::reference_fee_units;
    auto const seq = lastClosedLedger->info().seq + 1;

    // add transactions to our position
    if ((baseFee != lastClosedLedger->fees().base) ||
        (baseReserve != lastClosedLedger->fees().accountReserve(0)) ||
            (incReserve != lastClosedLedger->fees().increment))
    {
        JLOG(journal_.warn()) <<
            "We are voting for a fee change: " << baseFee <<
            "/" << baseReserve <<
            "/" << incReserve;

        STTx feeTx (ttFEE,
            [seq,baseFee,baseReserve,incReserve,feeUnits](auto& obj)
            {
                obj[sfAccount] = AccountID();
                obj[sfLedgerSequence] = seq;
                obj[sfBaseFee] = baseFee;
                obj[sfReserveBase] = baseReserve;
                obj[sfReserveIncrement] = incReserve;
                obj[sfReferenceFeeUnits] = feeUnits;
            });

        uint256 txID = feeTx.getTransactionID ();

        JLOG(journal_.warn()) <<
            "Vote: " << txID;

        Serializer s;
        feeTx.add (s);

        auto tItem = std::make_shared<SHAMapItem> (txID, s.peekData ());

        if (!initialPosition->addGiveItem (tItem, true, false))
        {
            JLOG(journal_.warn()) <<
                "Ledger already had fee change";
        }
    }
}

//------------------------------------------------------------------------------

FeeVote::Setup
setup_FeeVote (Section const& section)
{
    FeeVote::Setup setup;
    set (setup.reference_fee, "reference_fee", section);
    set (setup.account_reserve, "account_reserve", section);
    set (setup.owner_reserve, "owner_reserve", section);
    return setup;
}

std::unique_ptr<FeeVote>
make_FeeVote (FeeVote::Setup const& setup, beast::Journal journal)
{
    return std::make_unique<FeeVoteImpl> (setup, journal);
}

} // ripple
