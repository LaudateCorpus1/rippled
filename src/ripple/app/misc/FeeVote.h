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

#ifndef RIPPLE_APP_MISC_FEEVOTE_H_INCLUDED
#define RIPPLE_APP_MISC_FEEVOTE_H_INCLUDED

#include <ripple/ledger/ReadView.h>
#include <ripple/shamap/SHAMap.h>
#include <ripple/protocol/STValidation.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/protocol/SystemParameters.h>

namespace ripple {

/** Manager to process fee votes. */
class FeeVote
{
public:
    /** Fee schedule to vote for.
        During voting ledgers, the FeeVote logic will try to move towards
        these values when injecting fee-setting transactions.
        A default-constructed Setup contains recommended values.
    */
    struct Setup
    {
        /** The cost of a reference transaction in drops. */
        XRPAmountU64 reference_fee{ 10 };

        /** The cost of a reference transaction in fee units. */
        static constexpr FeeUnit32 reference_fee_units{ 10 };

        /** The account reserve requirement in drops. */
        XRPAmountU32 account_reserve{ 20 * DROPS_PER_XRP };

        /** The per-owned item reserve requirement in drops. */
        XRPAmountU32 owner_reserve{ 5 * DROPS_PER_XRP };
    };

    virtual ~FeeVote () = default;

    /** Add local fee preference to validation.

        @param lastClosedLedger
        @param baseValidation
    */
    virtual
    void
    doValidation (std::shared_ptr<ReadView const> const& lastClosedLedger,
        STValidation::FeeSettings & fees) = 0;

    /** Cast our local vote on the fee.

        @param lastClosedLedger
        @param initialPosition
    */
    virtual
    void
    doVoting (std::shared_ptr<ReadView const> const& lastClosedLedger,
        std::vector<STValidation::pointer> const& parentValidations,
            std::shared_ptr<SHAMap> const& initialPosition) = 0;
};

/** Build FeeVote::Setup from a config section. */
FeeVote::Setup
setup_FeeVote (Section const& section);

/** Create an instance of the FeeVote logic.
    @param setup The fee schedule to vote for.
    @param journal Where to log.
*/
std::unique_ptr <FeeVote>
make_FeeVote (FeeVote::Setup const& setup, beast::Journal journal);

} // ripple

#endif
