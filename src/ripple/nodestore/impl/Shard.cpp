//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2017 Ripple Labs Inc.

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

#include <ripple/nodestore/impl/Shard.h>
#include <ripple/app/ledger/InboundLedger.h>
#include <ripple/app/main/DBInit.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/core/ConfigSections.h>
#include <ripple/nodestore/impl/DatabaseShardImp.h>
#include <ripple/nodestore/Manager.h>

#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <fstream>

namespace ripple {
namespace NodeStore {

Shard::Shard(
    Application& app,
    DatabaseShard const& db,
    std::uint32_t index,
    beast::Journal& j)
    : app_(app)
    , index_(index)
    , firstSeq_(db.firstLedgerSeq(index))
    , lastSeq_(std::max(firstSeq_, db.lastLedgerSeq(index)))
    , maxLedgers_(index == db.earliestShardIndex() ?
        lastSeq_ - firstSeq_ + 1 : db.ledgersPerShard())
    , dir_(db.getRootDir() / std::to_string(index_))
    , control_(dir_ / controlFileName)
    , j_(j)
{
    if (index_ < db.earliestShardIndex())
        Throw<std::runtime_error>("Shard: Invalid index");
}

bool
Shard::open(Scheduler& scheduler, nudb::context& ctx)
{
    std::lock_guard<std::mutex> lock(mutex_);
    assert(!backend_);

    Config const& config {app_.config()};
    Section section {config.section(ConfigSection::shardDatabase())};
    std::string const type (get<std::string>(section, "type", "nudb"));
    auto factory {Manager::instance().find(type)};
    if (!factory)
    {
        JLOG(j_.error()) <<
            "shard " << index_ <<
            " failed to create backend type " << type;
        return false;
    }

    section.set("path", dir_.string());
    backend_ = factory->createInstance(
        NodeObject::keyBytes, section, scheduler, ctx, j_);

    auto const preexist {exists(dir_)};
    auto fail = [this, preexist](std::string const& msg)
    {
        pCache_.reset();
        nCache_.reset();
        backend_.reset();
        lgrSQLiteDB_.reset();
        txSQLiteDB_.reset();
        storedSeqs_.clear();
        lastStored_.reset();

        if (!preexist)
            removeAll(dir_, j_);

        if (!msg.empty())
        {
            JLOG(j_.error()) <<
                "shard " << index_ << " " << msg;
        }
        return false;
    };

    try
    {
        // Open/Create the NuDB key/value store for node objects
        backend_->open(!preexist);

        if (!backend_->backed())
            return true;

        if (!preexist)
        {
            // New shard, create a control file
            if (!saveControl(lock))
                return fail({});
        }
        else if (is_regular_file(control_))
        {
            // Incomplete shard, inspect control file
            std::ifstream ifs(control_.string());
            if (!ifs.is_open())
                return fail("failed to open control file");

            boost::archive::text_iarchive ar(ifs);
            ar & storedSeqs_;
            if (!storedSeqs_.empty())
            {
                if (boost::icl::first(storedSeqs_) < firstSeq_ ||
                    boost::icl::last(storedSeqs_) > lastSeq_)
                {
                    return fail("has an invalid control file");
                }

                if (boost::icl::length(storedSeqs_) >= maxLedgers_)
                {
                    JLOG(j_.warn()) <<
                        "shard " << index_ <<
                        " has a control file for complete shard";
                    setComplete(lock);
                }
            }
        }
        else
            setComplete(lock);

        if (!complete_)
        {
            setCache(lock);
            if (!initSQLite(lock) ||!setFileStats(lock))
                return fail({});
        }
    }
    catch (std::exception const& e)
    {
        return fail(std::string("exception ") +
            e.what() + " in function " + __func__);
    }

    return true;
}

bool
Shard::setStored(std::shared_ptr<Ledger const> const& ledger)
{
    std::lock_guard<std::mutex> lock(mutex_);
    assert(backend_ && !complete_);

    if (boost::icl::contains(storedSeqs_, ledger->info().seq))
    {
        JLOG(j_.debug()) <<
            "shard " << index_ <<
            " has ledger sequence " << ledger->info().seq << " already stored";
        return false;
    }

    if (!setSQLiteStored(ledger, lock))
        return false;

    // Check if the shard is complete
    if (boost::icl::length(storedSeqs_) >= maxLedgers_ - 1)
        setComplete(lock);
    else
    {
        storedSeqs_.insert(ledger->info().seq);
        if (backend_->backed() && !saveControl(lock))
            return false;
    }

    JLOG(j_.debug()) <<
        "shard " << index_ <<
        " stored ledger sequence " << ledger->info().seq <<
        (complete_ ? " and is complete" : "");

    lastStored_ = ledger;
    return true;
}

boost::optional<std::uint32_t>
Shard::prepare()
{
    std::lock_guard<std::mutex> lock(mutex_);
    assert(backend_ && !complete_);

    if (storedSeqs_.empty())
         return lastSeq_;
    return prevMissing(storedSeqs_, 1 + lastSeq_, firstSeq_);
}

bool
Shard::contains(std::uint32_t seq) const
{
    assert(backend_);
    if (seq < firstSeq_ || seq > lastSeq_)
        return false;

    std::lock_guard<std::mutex> lock(mutex_);
    return complete_ || boost::icl::contains(storedSeqs_, seq);
}

void
Shard::sweep()
{
    assert(backend_);
    pCache_->sweep();
    nCache_->sweep();
}

bool
Shard::validate()
{
    uint256 hash;
    std::uint32_t seq;
    std::shared_ptr<Ledger> ledger;
    auto fail = [this](std::string const& msg)
    {
        JLOG(j_.error()) << "shard " << index_ << " " << msg;
        return false;
    };

    // Find the hash of the last ledger in this shard
    {
        std::tie(ledger, seq, hash) = loadLedgerHelper(
            "WHERE LedgerSeq >= " + std::to_string(lastSeq_) +
            " order by LedgerSeq desc limit 1", app_, false);
        if (!ledger)
            return fail("is unable to validate due to lacking lookup data");

        if (seq != lastSeq_)
        {
            ledger->setImmutable(app_.config());
            boost::optional<uint256> h;

            try
            {
                h = hashOfSeq(*ledger, lastSeq_, j_);
            }
            catch (std::exception const& e)
            {
                return fail(std::string("exception ") +
                    e.what() + " in function " + __func__);
            }

            if (!h)
            {
                return fail("is missing hash for last ledger sequence " +
                    std::to_string(lastSeq_));
            }
            hash = *h;
            seq = lastSeq_;
        }
    }

    JLOG(j_.debug()) <<
        "shard " << index_ <<
        " has ledger sequences " << firstSeq_ << "-" << lastSeq_;

    // Use a short age to keep memory consumption low
    auto const savedAge {pCache_->getTargetAge()};
    using namespace std::chrono_literals;
    pCache_->setTargetAge(1s);

    // Validate every ledger stored in this shard
    std::shared_ptr<Ledger const> next;
    while (seq >= firstSeq_)
    {
        auto nObj = valFetch(hash);
        if (!nObj)
            break;
        ledger = std::make_shared<Ledger>(
            InboundLedger::deserializeHeader(makeSlice(nObj->getData()),
                true), app_.config(), *app_.shardFamily());
        if (ledger->info().seq != seq)
        {
            fail("encountered invalid ledger sequence " + std::to_string(seq));
            break;
        }
        if (ledger->info().hash != hash)
        {
            fail("encountered invalid ledger hash " + to_string(hash) +
                " on sequence " + std::to_string(seq));
            break;
        }
        ledger->stateMap().setLedgerSeq(seq);
        ledger->txMap().setLedgerSeq(seq);
        ledger->setImmutable(app_.config());
        if (!ledger->stateMap().fetchRoot(
            SHAMapHash {ledger->info().accountHash}, nullptr))
        {
            fail("is missing root STATE node on sequence " +
                std::to_string(seq));
            break;
        }
        if (ledger->info().txHash.isNonZero())
        {
            if (!ledger->txMap().fetchRoot(
                SHAMapHash {ledger->info().txHash}, nullptr))
            {
                fail("is missing root TXN node on sequence " +
                    std::to_string(seq));
                break;
            }
        }
        if (!valLedger(ledger, next))
            break;
        hash = ledger->info().parentHash;
        --seq;
        next = ledger;
        if (seq % 128 == 0)
            pCache_->sweep();
    }

    pCache_->reset();
    nCache_->reset();
    pCache_->setTargetAge(savedAge);

    if (seq >= firstSeq_)
    {
        return fail(std::string(" is ") +
            (complete_ ? "invalid, failed" : "incomplete, stopped") +
            " on hash " + to_string(hash) + " on sequence " +
            std::to_string(seq));
    }

    JLOG(j_.debug()) <<
        "shard " << index_ << " is valid and complete";
    return true;
}

bool
Shard::valLedger(std::shared_ptr<Ledger const> const& ledger,
    std::shared_ptr<Ledger const> const& next)
{
    auto fail = [this](std::string const& msg)
    {
        JLOG(j_.error()) << "shard " << index_ << " " << msg;
        return false;
    };

    if (ledger->info().hash.isZero())
    {
        return fail("encountered a zero ledger hash on sequence " +
            std::to_string(ledger->info().seq));
    }
    if (ledger->info().accountHash.isZero())
    {
        return fail("encountered a zero account hash on sequence " +
            std::to_string(ledger->info().seq));
    }

    bool error {false};
    auto f = [this, &error](SHAMapAbstractNode& node)
    {
        if (!valFetch(node.getNodeHash().as_uint256()))
            error = true;
        return !error;
    };

    // Validate the state map
    if (ledger->stateMap().getHash().isNonZero())
    {
        if (!ledger->stateMap().isValid())
        {
            return fail("has an invalid state map on sequence " +
                std::to_string(ledger->info().seq));
        }

        try
        {
            if (next && next->info().parentHash == ledger->info().hash)
                ledger->stateMap().visitDifferences(&next->stateMap(), f);
            else
                ledger->stateMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            return fail(std::string("exception ") +
                e.what() + " in function " + __func__);
        }
        if (error)
            return false;
    }
    // Validate the transaction map
    if (ledger->info().txHash.isNonZero())
    {
        if (!ledger->txMap().isValid())
        {
            return fail("has an invalid transaction map on sequence " +
                std::to_string(ledger->info().seq));
        }

        try
        {
            ledger->txMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            return fail(std::string("exception ") +
                e.what() + " in function " + __func__);
        }
        if (error)
            return false;
    }
    return true;
};

std::shared_ptr<NodeObject>
Shard::valFetch(uint256 const& hash)
{
    assert(backend_);
    std::shared_ptr<NodeObject> nObj;
    auto fail = [this, &nObj](std::string const& msg)
    {
        JLOG(j_.error()) << "shard " << index_ << " " << msg;
        nObj.reset();
        return nObj;
    };

    try
    {
        switch (backend_->fetch(hash.begin(), &nObj))
        {
        case ok:
            break;
        case notFound:
            return fail("is missing node object on hash " + to_string(hash));
        case dataCorrupt:
            return fail("has a corrupt node object on hash " + to_string(hash));
        default:
            return fail("encountered unknown error on hash " + to_string(hash));
        }
    }
    catch (std::exception const& e)
    {
        return fail(std::string("exception ") +
            e.what() + " in function " + __func__);
    }
    return nObj;
}

bool
Shard::setComplete(std::lock_guard<std::mutex> const& lock)
{
    // Remove the control file if one exists
    try
    {
        using namespace boost::filesystem;
        if (is_regular_file(control_))
            remove_all(control_);

    }
    catch (std::exception const& e)
    {
        JLOG(j_.error()) <<
            "shard " << index_ <<
            " exception " << e.what() <<
            " in function " << __func__;
        return false;
    }

    storedSeqs_.clear();
    complete_ = true;

    setCache(lock);
    return initSQLite(lock) && setFileStats(lock);
}

void
Shard::setCache(std::lock_guard<std::mutex> const&)
{
    // complete shards use the smallest cache and
    // fastest expiration to reduce memory consumption.
    // The incomplete shard is set according to configuration.
    if (!pCache_)
    {
        auto const name {"shard " + std::to_string(index_)};
        auto const sz {complete_ ?
            Config::getSize(siNodeCacheSize, 0) :
            app_.config().getSize(siNodeCacheSize)};
        auto const age {std::chrono::seconds{complete_ ?
            Config::getSize(siNodeCacheAge, 0) :
            app_.config().getSize(siNodeCacheAge)}};

        pCache_ = std::make_shared<PCache>(name, sz, age, stopwatch(), j_);
        nCache_ = std::make_shared<NCache>(name, stopwatch(), sz, age);
    }
    else
    {
        auto const sz {Config::getSize(siNodeCacheSize, 0)};
        pCache_->setTargetSize(sz);
        nCache_->setTargetSize(sz);

        auto const age {std::chrono::seconds{
            Config::getSize(siNodeCacheAge, 0)}};
        pCache_->setTargetAge(age);
        nCache_->setTargetAge(age);
    }
}

bool
Shard::initSQLite(std::lock_guard<std::mutex> const&)
{
    Config const& config {app_.config()};
    DatabaseCon::Setup setup;
    setup.startUp = config.START_UP;
    setup.standAlone = config.standalone();
    setup.dataDir = dir_;

    try
    {
        if (complete_)
        {
            // Remove WAL files if they exist
            using namespace boost::filesystem;
            for (auto const& d : directory_iterator(dir_))
            {
                if (is_regular_file(d) &&
                    boost::iends_with(extension(d), "-wal"))
                {
                    // Closing the session forces a checkpoint
                    if (!lgrSQLiteDB_)
                    {
                        lgrSQLiteDB_ = std::make_unique <DatabaseCon>(
                            setup,
                            LgrDBName,
                            LgrDBPragma,
                            LgrDBInit);
                    }
                    lgrSQLiteDB_->getSession().close();

                    if (!txSQLiteDB_)
                    {
                        txSQLiteDB_ = std::make_unique <DatabaseCon>(
                            setup,
                            TxDBName,
                            TxDBPragma,
                            TxDBInit);
                    }
                    txSQLiteDB_->getSession().close();
                    break;
                }
            }

            lgrSQLiteDB_ = std::make_unique <DatabaseCon>(
                setup,
                LgrDBName,
                CompleteShardDBPragma,
                LgrDBInit);
            lgrSQLiteDB_->getSession() <<
                boost::str(boost::format("PRAGMA cache_size=-%d;") %
                kilobytes(Config::getSize(siLgrDBCache, 0)));

            txSQLiteDB_ = std::make_unique <DatabaseCon>(
                setup,
                TxDBName,
                CompleteShardDBPragma,
                TxDBInit);
            txSQLiteDB_->getSession() <<
                boost::str(boost::format("PRAGMA cache_size=-%d;") %
                kilobytes(Config::getSize(siTxnDBCache, 0)));
        }
        else
        {
            // The incomplete shard uses a Write Ahead Log for performance
            lgrSQLiteDB_ = std::make_unique <DatabaseCon>(
                setup,
                LgrDBName,
                LgrDBPragma,
                LgrDBInit);
            lgrSQLiteDB_->getSession() <<
                boost::str(boost::format("PRAGMA cache_size=-%d;") %
                kilobytes(config.getSize(siLgrDBCache)));
            lgrSQLiteDB_->setupCheckpointing(&app_.getJobQueue(), app_.logs());

            txSQLiteDB_ = std::make_unique <DatabaseCon>(
                setup,
                TxDBName,
                TxDBPragma,
                TxDBInit);
            txSQLiteDB_->getSession() <<
                boost::str(boost::format("PRAGMA cache_size=-%d;") %
                kilobytes(config.getSize(siTxnDBCache)));
            txSQLiteDB_->setupCheckpointing(&app_.getJobQueue(), app_.logs());
        }
    }
    catch (std::exception const& e)
    {
        JLOG(j_.error()) <<
            "shard " << index_ <<
            " exception " << e.what() <<
            " in function " << __func__;
        return false;
    }
    return true;
}

bool
Shard::setSQLiteStored(
    std::shared_ptr<Ledger const> const& ledger,
    std::lock_guard<std::mutex> const&)
{
    auto const seq {ledger->info().seq};
    assert(backend_ && !complete_);
    assert(!boost::icl::contains(storedSeqs_, seq));

    try
    {
        {
            auto& session {txSQLiteDB_->getSession()};
            soci::transaction tr(session);

            session <<
                "DELETE FROM Transactions WHERE LedgerSeq = :seq;"
                , soci::use(seq);
            session <<
                "DELETE FROM AccountTransactions WHERE LedgerSeq = :seq;"
                , soci::use(seq);

            if (ledger->info().txHash.isNonZero())
            {
                auto const sSeq {std::to_string(seq)};
                if (!ledger->txMap().isValid())
                {
                    JLOG(j_.error()) <<
                        "shard " << index_ <<
                        " has an invalid transaction map" <<
                        " on sequence " << sSeq;
                    return false;
                }

                for (auto const& item : ledger->txs)
                {
                    auto const txID {item.first->getTransactionID()};
                    auto const sTxID {to_string(txID)};
                    auto const txMeta {std::make_shared<TxMeta>(
                        txID, ledger->seq(), *item.second)};

                    session <<
                        "DELETE FROM AccountTransactions WHERE TransID = :txID;"
                        , soci::use(sTxID);

                    auto const& accounts = txMeta->getAffectedAccounts(j_);
                    if (!accounts.empty())
                    {
                        auto const s(boost::str(boost::format(
                            "('%s','%s',%s,%s)")
                            % sTxID
                            % "%s"
                            % sSeq
                            % std::to_string(txMeta->getIndex())));
                        std::string sql;
                        sql.reserve((accounts.size() + 1) * 128);
                        sql = "INSERT INTO AccountTransactions "
                            "(TransID, Account, LedgerSeq, TxnSeq) VALUES ";
                        sql += boost::algorithm::join(
                            accounts | boost::adaptors::transformed(
                                [&](AccountID const& accountID)
                                {
                                    return boost::str(boost::format(s)
                                        % ripple::toBase58(accountID));
                                }),
                                ",");
                        sql += ';';
                        session << sql;

                        JLOG(j_.trace()) <<
                            "shard " << index_ <<
                            " account transaction: " << sql;
                    }
                    else
                    {
                        JLOG(j_.warn()) <<
                            "shard " << index_ <<
                            " transaction in ledger " << sSeq <<
                            " affects no accounts";
                    }

                    Serializer s;
                    item.second->add(s);
                    session <<
                       (STTx::getMetaSQLInsertReplaceHeader() +
                           item.first->getMetaSQL(
                               seq,
                               sqlEscape(std::move(s.modData())))
                           + ';');
                }
            }

            tr.commit ();
        }

        auto& session {lgrSQLiteDB_->getSession()};
        soci::transaction tr(session);

        session <<
            "DELETE FROM Ledgers WHERE LedgerSeq = :seq;"
            , soci::use(seq);
        session <<
            "INSERT OR REPLACE INTO Ledgers ("
                "LedgerHash, LedgerSeq, PrevHash, TotalCoins, ClosingTime,"
                "PrevClosingTime, CloseTimeRes, CloseFlags, AccountSetHash,"
                "TransSetHash)"
            "VALUES ("
                ":ledgerHash, :ledgerSeq, :prevHash, :totalCoins, :closingTime,"
                ":prevClosingTime, :closeTimeRes, :closeFlags, :accountSetHash,"
                ":transSetHash);",
            soci::use(to_string(ledger->info().hash)),
            soci::use(seq),
            soci::use(to_string(ledger->info().parentHash)),
            soci::use(to_string(ledger->info().drops)),
            soci::use(ledger->info().closeTime.time_since_epoch().count()),
            soci::use(ledger->info().parentCloseTime.time_since_epoch().count()),
            soci::use(ledger->info().closeTimeResolution.count()),
            soci::use(ledger->info().closeFlags),
            soci::use(to_string(ledger->info().accountHash)),
            soci::use(to_string(ledger->info().txHash));

        tr.commit();
    }
    catch (std::exception const& e)
    {
        JLOG(j_.error()) <<
            "shard " << index_ <<
            " exception " << e.what() <<
            " in function " << __func__;
        return false;
    }
    return true;
}

bool
Shard::setFileStats(std::lock_guard<std::mutex> const&)
{
    fileSz_ = 0;
    fdRequired_ = 0;
    if (backend_->backed())
    {
        try
        {
            using namespace boost::filesystem;
            for (auto const& d : directory_iterator(dir_))
            {
                if (is_regular_file(d))
                {
                    fileSz_ += file_size(d);
                    ++fdRequired_;
                }
            }
        }
        catch (std::exception const& e)
        {
            JLOG(j_.error()) <<
                "shard " << index_ <<
                " exception " << e.what() <<
                " in function " << __func__;
            return false;
        }
    }
    return true;
}

bool
Shard::saveControl(std::lock_guard<std::mutex> const&)
{
    std::ofstream ofs {control_.string(), std::ios::trunc};
    if (!ofs.is_open())
    {
        JLOG(j_.fatal()) <<
            "shard " << index_ << " is unable to save control file";
        return false;
    }

    boost::archive::text_oarchive ar(ofs);
    ar & storedSeqs_;
    return true;
}

} // NodeStore
} // ripple
