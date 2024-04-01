import re
from typing import Optional, Dict

# Credit: https://gist.github.com/MBrassey/623f7b8d02766fa2d826bf9eca3fe005
CRYPTOS = {
    "ETH": "0x[a-fA-F0-9]{40}",
    "BTC": "bc1[a-zA-HJ-NP-Z0-9]{25,39}|[13][a-km-zA-HJ-NP-Z1-9]{25,39}",
    "USDT": "0x[a-fA-F0-9]{40}",
    "DASH": "X[1-9A-HJ-NP-Za-km-z]{33}",
    "XMR": "4[0-9AB][1-9A-HJ-NP-Za-km-z]{93}",
    "XRP": "r[0-9a-zA-Z]{24,34}",
    "LTC": "ltc1[a-zA-HJ-NP-Z0-9]{25,39}|[LM3][a-km-zA-HJ-NP-Z1-9]{25,39}",
    "BCH": "bitcoincash:q[a-z0-9]{41}",
    "DOGE": "D{1}[5-9A-HJ-NP-U]{1}[1-9A-HJ-NP-Za-km-z]{32}",
    "TRON": "T[1-9A-HJ-NP-Za-km-z]{33}",
}
CRYPTOS_RE = {k: re.compile(v) for k, v in CRYPTOS.items()}


def extract_cryptos(text: Optional[str]) -> Dict[str, str]:
    """Extract cryptocurrency addresses from text.

    Args:
        text: The text to extract from.

    Returns:
        A set of cryptocurrency IDs, with currency code.
    """
    out: Dict[str, str] = {}
    if text is None:
        return out
    for currency, v in CRYPTOS_RE.items():
        for key in v.findall(text):
            out[key] = currency
    return out
