//------------------------------------------------------------------------------
/*
    This file is part of Beast: https://github.com/vinniefalco/Beast
    Copyright 2014, Vinnie Falco <vinnie.falco@gmail.com>

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

#ifndef BEAST_NUDB_VARINT_H_INCLUDED
#define BEAST_NUDB_VARINT_H_INCLUDED

#include <nudb/detail/stream.hpp>
#include <ripple/basics/varint_common.h>

namespace ripple {
namespace NodeStore {

// input stream

template <class T, std::enable_if_t<
    std::is_same<T, varint>::value>* = nullptr>
void
read (nudb::detail::istream& is, std::size_t& u)
{
    auto p0 = is(1);
    auto p1 = p0;
    while (*p1++ & 0x80)
        is(1);
    read_varint(p0, p1 - p0, u);
}

// output stream

template <class T, std::enable_if_t<
    std::is_same<T, varint>::value>* = nullptr>
void
write (nudb::detail::ostream& os, std::size_t t)
{
    write_varint(os.data(
        size_varint(t)), t);
}

} // NodeStore
} // ripple

#endif
