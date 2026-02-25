import re
from typing import Optional, Dict

# Credit: https://gist.github.com/MBrassey/623f7b8d02766fa2d826bf9eca3fe005

# Add word boundaries (\b) to prevent matching partial cryptocurrency addresses
# embedded within longer strings. Ensures we extract only complete, standalone
# addresses rather than substrings that would be cut off or invalid.
CRYPTOS = {
    "ETH": r"\b0x[a-fA-F0-9]{40}\b",
    "BTC": r"\b(?:bc1[a-zA-HJ-NP-Z0-9]{25,39}|[13][a-km-zA-HJ-NP-Z1-9]{25,39})\b",
    "DASH": r"\bX[1-9A-HJ-NP-Za-km-z]{33}\b",
    "XMR": r"\b4[0-9AB][1-9A-HJ-NP-Za-km-z]{93}\b",
    "XRP": r"\br[0-9a-zA-Z]{24,34}\b",
    "LTC": r"\b(?:ltc1[a-zA-HJ-NP-Z0-9]{25,39}|[LM3][a-km-zA-HJ-NP-Z1-9]{25,39})\b",
    "BCH": r"\bbitcoincash:q[a-z0-9]{41}\b",
    "DOGE": r"\bD{1}[5-9A-HJ-NP-U]{1}[1-9A-HJ-NP-Za-km-z]{32}\b",
    "TRON": r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b",
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
