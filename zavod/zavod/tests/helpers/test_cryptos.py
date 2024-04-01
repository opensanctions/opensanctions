from zavod.helpers.crypto import extract_cryptos


def test_extract_cryptos():
    assert len(extract_cryptos(None)) == 0
    assert len(extract_cryptos("ETH")) == 0

    msg = "Buy drugs: 0x1234567890123456789012345678901234567890"
    assert len(extract_cryptos(msg)) == 1

    teth = "x 0xdAC17F958D2ee523a2206206994597C13D831ec7"
    out = extract_cryptos(teth)
    assert len(out) == 1
    assert "0xdAC17F958D2ee523a2206206994597C13D831ec7" in out
    assert "USDT" in out.values()

    tron = "hamas TXRTT6K7djpRGvYqiMHZSc9HcCN9dCQqu7"
    out = extract_cryptos(tron)
    assert len(out) == 1
    assert "TXRTT6K7djpRGvYqiMHZSc9HcCN9dCQqu7" in out
    assert "TRON" in out.values()
