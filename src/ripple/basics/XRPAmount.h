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

#ifndef RIPPLE_BASICS_XRPAMOUNT_H_INCLUDED
#define RIPPLE_BASICS_XRPAMOUNT_H_INCLUDED

#include <ripple/basics/contract.h>
#include <ripple/beast/cxx17/type_traits.h>
#include <ripple/beast/utility/Zero.h>
#include <ripple/json/json_value.h>
#include <boost/operators.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <cstdint>
#include <string>

namespace ripple {

namespace feeunit {

/** "drops" are the smallest divisible amount of XRP. This is what most
    of the code uses. */
struct drop_tag;

} // feeunit

template<class T>
class XRPAmountBase
    : private boost::totally_ordered <XRPAmountBase<T>>
    , private boost::additive <XRPAmountBase<T>>
    , private boost::dividable <XRPAmountBase<T>, T>
    , private boost::modable <XRPAmountBase<T>, T>
    , private boost::unit_steppable <XRPAmountBase<T>>
{
public:
    using unit_type = feeunit::drop_tag;
    using value_type = T;
private:
    value_type drops_;

protected:
    template<class Other>
    static constexpr bool is_compatible_v = std::is_integral_v<Other>;

    template<class Other>
    using enable_if_compatible_t =
        typename std::enable_if_t<is_compatible_v<Other>>;

public:
    XRPAmountBase () = default;
    constexpr XRPAmountBase (XRPAmountBase const& other) = default;
    constexpr XRPAmountBase& operator= (XRPAmountBase const& other) = default;

    constexpr
    XRPAmountBase (beast::Zero)
        : drops_ (0)
    {
        static_assert(std::is_convertible_v<value_type, std::uint64_t>,
            "conversion");
    }

    constexpr
    XRPAmountBase&
    operator= (beast::Zero)
    {
        drops_ = 0;
        return *this;
    }

    constexpr
    XRPAmountBase (value_type drops)
        : drops_ (drops)
    {
    }

    template <class Other,
        class = enable_if_compatible_t <Other>>
    constexpr
    XRPAmountBase (Other drops)
        : drops_ (static_cast<value_type> (drops))
    {
    }

    template <class Other,
        class = enable_if_compatible_t <Other>>
    XRPAmountBase&
    operator= (Other drops)
    {
        drops_ = static_cast<value_type> (drops);
        return *this;
    }

    template <class Other,
        class = enable_if_compatible_t <Other>>
    explicit
    constexpr
    XRPAmountBase (XRPAmountBase<Other> drops)
        : drops_ (static_cast<value_type> (drops.drops()))
    {
    }

    constexpr
    XRPAmountBase
    operator*(value_type const& rhs) const
    {
        return { drops_ * rhs };
    }

    friend
    constexpr
    XRPAmountBase
    operator*(value_type lhs, XRPAmountBase const& rhs)
    {
        // multiplication is commutative
        return rhs * lhs;
    }

    constexpr
    value_type
    operator/(XRPAmountBase const& rhs) const
    {
        return drops_ / rhs.drops_;
    }

    XRPAmountBase&
    operator+= (XRPAmountBase const& other)
    {
        drops_ += other.drops();
        return *this;
    }

    XRPAmountBase&
    operator-= (XRPAmountBase const& other)
    {
        drops_ -= other.drops();
        return *this;
    }

    XRPAmountBase&
    operator++()
    {
        ++drops_;
        return *this;
    }

    XRPAmountBase&
    operator--()
    {
        --drops_;
        return *this;
    }

    XRPAmountBase&
    operator*= (value_type const& rhs)
    {
        drops_ *= rhs;
        return *this;
    }

    XRPAmountBase&
    operator/= (value_type const& rhs)
    {
        drops_ /= rhs;
        return *this;
    }

    XRPAmountBase&
    operator%= (value_type const& rhs)
    {
        drops_ %= rhs;
        return *this;
    }

    XRPAmountBase
    operator- () const
    {
        return { -drops_ };
    }

    bool
    operator==(XRPAmountBase const& other) const
    {
        return drops_ == other.drops_;
    }

    template <class Other,
        class = enable_if_compatible_t <Other>>
    bool
    operator==(XRPAmountBase<Other> const& other) const
    {
        return drops_ == other.drops();
    }

    template <class Other,
        class = enable_if_compatible_t <Other>>
    bool
    operator!=(XRPAmountBase<Other> const& other) const
    {
        return !operator==(other);
    }

    bool
    operator<(XRPAmountBase const& other) const
    {
        return drops_ < other.drops_;
    }

    /** Returns true if the amount is not zero */
    explicit
    constexpr
    operator bool() const noexcept
    {
        return drops_ != 0;
    }

    /** Return the sign of the amount */
    constexpr
    int
    signum() const noexcept
    {
        return (drops_ < 0) ? -1 : (drops_ ? 1 : 0);
    }

    /** Returns the number of drops */
    constexpr
    value_type
    drops () const
    {
        return drops_;
    }

    constexpr
    double
    decimalXRP () const;

    Json::Value
    json () const
    {
        static_assert(std::is_signed_v<value_type> &&
            std::is_integral_v<value_type>,
            "Expected XRPAmount to be a signed integral type");

        constexpr auto min = std::numeric_limits<Json::Int>::min();
        constexpr auto max = std::numeric_limits<Json::Int>::max();

        if (drops_ < min)
            return min;
        if (drops_ > max)
            return max;
        return static_cast<Json::Int>(drops_);
    }

    /** Returns the underlying value. Code SHOULD NOT call this
        function unless the type has been abstracted away,
        e.g. in a templated function.
    */
    constexpr
    value_type
    value () const
    {
        return drops_;
    }

    friend
    std::istream&
    operator>> (std::istream& s, XRPAmountBase& val)
    {
        s >> val.drops_;
        return s;
    }

};

/** Number of drops per 1 XRP */
template<class T>
constexpr
XRPAmountBase<T>
DropsPerXRP{1'000'000};

template<class T>
constexpr
double
XRPAmountBase<T>::decimalXRP () const
{
    return static_cast<double>(drops_) / DropsPerXRP<T>.drops();
}

using XRPAmount = XRPAmountBase<std::int64_t>;
using XRPAmountU32 = XRPAmountBase<std::uint32_t>;
using XRPAmountU64 = XRPAmountBase<std::uint64_t>;

constexpr
XRPAmount
DROPS_PER_XRP{DropsPerXRP<XRPAmount::value_type>};

// Output XRPAmount as just the drops value.
template<class Char, class Traits, class T>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& os,
    const XRPAmountBase<T>& q)
{
    return os << q.drops();
}

inline
std::string
to_string (XRPAmount const& amount)
{
    return std::to_string (amount.drops ());
}

inline
XRPAmount
mulRatio (
    XRPAmount const& amt,
    std::uint32_t num,
    std::uint32_t den,
    bool roundUp)
{
    using namespace boost::multiprecision;

    if (!den)
        Throw<std::runtime_error> ("division by zero");

    int128_t const amt128 (amt.drops ());
    auto const neg = amt.drops () < 0;
    auto const m = amt128 * num;
    auto r = m / den;
    if (m % den)
    {
        if (!neg && roundUp)
            r += 1;
        if (neg && !roundUp)
            r -= 1;
    }
    if (r > std::numeric_limits<XRPAmount::value_type>::max ())
        Throw<std::overflow_error> ("XRP mulRatio overflow");
    // TODO: DO WE NEED AN AMENDMENT TO ADD THIS CHECK
    //if (r < std::numeric_limits<XRPAmount::value_type>::min ())
    //    Throw<std::overflow_error> ("XRP mulRatio underflow");
    return XRPAmount (r.convert_to<XRPAmount::value_type> ());
}

}

#endif // RIPPLE_BASICS_XRPAMOUNT_H_INCLUDED
