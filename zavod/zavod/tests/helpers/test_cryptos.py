from zavod.helpers.crypto import extract_cryptos


def test_extract_cryptos():
    assert len(extract_cryptos(None)) == 0
    assert len(extract_cryptos("ETH")) == 0

    msg = "Buy drugs: 0x1234567890123456789012345678901234567890"
    assert len(extract_cryptos(msg)) == 1

    tron = "hamas TXRTT6K7djpRGvYqiMHZSc9HcCN9dCQqu7"
    out = extract_cryptos(tron)
    assert len(out) == 1
    assert "TXRTT6K7djpRGvYqiMHZSc9HcCN9dCQqu7" in out
    assert "TRON" in out.values()

    eth = "Here is my 0x8145D05037d1778E232ACE2FaF9731a6E5b19538 Ethereum address."
    out = extract_cryptos(eth)
    assert "0x8145D05037d1778E232ACE2FaF9731a6E5b19538" in out
    assert "ETH" in out.values()
