"""Symbol — canonical trading pair representation."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, field_validator

__all__ = ["Symbol"]

_ALIASES: dict[str, str] = {"XBT": "BTC"}


class Symbol(BaseModel, frozen=True):
    """Canonical trading pair (base/quote, spot by default).

    Examples
    --------
    >>> Symbol(base='BTC', quote='USDT')
    Symbol(base='BTC', quote='USDT', market='spot')
    >>> str(Symbol(base='BTC', quote='USDT'))
    'BTC/USDT'
    >>> Symbol.parse('BTC/USDT')
    Symbol(base='BTC', quote='USDT', market='spot')
    >>> Symbol.parse('XBT/USD')
    Symbol(base='BTC', quote='USD', market='spot')
    """

    base: str
    quote: str
    market: Literal["spot"] = "spot"

    @field_validator("base", "quote", mode="before")
    @classmethod
    def _normalize(cls, v: str) -> str:
        v = v.upper().strip()
        return _ALIASES.get(v, v)

    def __str__(self) -> str:
        return f"{self.base}/{self.quote}"

    def __repr__(self) -> str:
        return f"Symbol(base={self.base!r}, quote={self.quote!r}, market={self.market!r})"

    @classmethod
    def parse(cls, raw: str, market: Literal["spot"] = "spot") -> "Symbol":
        """Parse a pair string with an optional separator.

        Accepted formats: ``BTC/USDT``, ``BTC-USDT``, ``BTCUSDT`` (ambiguous,
        use explicit separator when possible).

        Examples
        --------
        >>> Symbol.parse('ETH-USD')
        Symbol(base='ETH', quote='USD', market='spot')
        """
        for sep in ("/", "-", "_"):
            if sep in raw:
                base, quote = raw.split(sep, 1)
                return cls(base=base, quote=quote, market=market)
        raise ValueError(
            f"Cannot parse symbol {raw!r}: use a separator like '/' or '-'"
        )
