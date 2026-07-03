"""Symbol — canonical trading pair representation."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, field_validator

__all__ = ["Symbol"]

_ALIASES: dict[str, str] = {"XBT": "BTC"}


_MARKETS: tuple[str, ...] = ("spot", "perp", "quarter", "next_quarter")


class Symbol(BaseModel, frozen=True):
    """Canonical trading pair (base/quote, spot by default).

    ``market`` addresses non-spot derivative markets (``perp``, ``quarter``,
    ``next_quarter``); ``str()`` only appends the ``:<market>`` suffix when
    it is not ``spot``, so existing spot-only code paths (storage layout,
    job ids) are unaffected.

    Examples
    --------
    >>> Symbol(base='BTC', quote='USDT')
    Symbol(base='BTC', quote='USDT', market='spot')
    >>> str(Symbol(base='BTC', quote='USDT'))
    'BTC/USDT'
    >>> str(Symbol(base='BTC', quote='USDT', market='perp'))
    'BTC/USDT:perp'
    >>> Symbol.parse('BTC/USDT')
    Symbol(base='BTC', quote='USDT', market='spot')
    >>> Symbol.parse('XBT/USD')
    Symbol(base='BTC', quote='USD', market='spot')
    >>> Symbol.parse('BTC/USDT:perp')
    Symbol(base='BTC', quote='USDT', market='perp')
    """

    base: str
    quote: str
    market: Literal["spot", "perp", "quarter", "next_quarter"] = "spot"

    @field_validator("base", "quote", mode="before")
    @classmethod
    def _normalize(cls, v: str) -> str:
        v = v.upper().strip()
        return _ALIASES.get(v, v)

    def __str__(self) -> str:
        if self.market == "spot":
            return f"{self.base}/{self.quote}"
        return f"{self.base}/{self.quote}:{self.market}"

    def __repr__(self) -> str:
        return f"Symbol(base={self.base!r}, quote={self.quote!r}, market={self.market!r})"

    @classmethod
    def parse(
        cls,
        raw: str,
        market: Literal["spot", "perp", "quarter", "next_quarter"] = "spot",
    ) -> "Symbol":
        """Parse a pair string with an optional separator and market suffix.

        Accepted formats: ``BTC/USDT``, ``BTC-USDT``, ``BTCUSDT`` (ambiguous,
        use explicit separator when possible), each optionally followed by
        ``:<market>`` (e.g. ``BTC/USDT:perp``). An explicit suffix wins over
        the ``market`` keyword argument.

        Parameters
        ----------
        raw : str
            Pair string, optionally suffixed with ``:<market>``.
        market : 'spot', 'perp', 'quarter', or 'next_quarter'
            Market to use when ``raw`` carries no ``:<market>`` suffix.

        Examples
        --------
        >>> Symbol.parse('ETH-USD')
        Symbol(base='ETH', quote='USD', market='spot')
        >>> Symbol.parse('ETH-USD:quarter')
        Symbol(base='ETH', quote='USD', market='quarter')
        """
        pair = raw
        if ":" in raw:
            pair, suffix = raw.split(":", 1)
            if suffix not in _MARKETS:
                raise ValueError(
                    f"Unknown market {suffix!r} in {raw!r}. "
                    f"Supported: {_MARKETS}"
                )
            market = suffix  # type: ignore[assignment]
        for sep in ("/", "-", "_"):
            if sep in pair:
                base, quote = pair.split(sep, 1)
                return cls(base=base, quote=quote, market=market)
        raise ValueError(
            f"Cannot parse symbol {raw!r}: use a separator like '/' or '-'"
        )
