from zavod.helpers.crypto import extract_cryptos


def test_extract_cryptos():
    # Basic tests
    assert len(extract_cryptos(None)) == 0
    assert len(extract_cryptos("")) == 0
    assert len(extract_cryptos("ETH")) == 0
    assert len(extract_cryptos("random text 123456")) == 0

    # Ethereum
    msg = "Buy drugs: 0xe090669ee62e02f4437b89058a073dc7874aed8f"
    result = extract_cryptos(msg)
    assert len(result) == 1
    assert "0xe090669ee62e02f4437b89058a073dc7874aed8f" in result
    assert result["0xe090669ee62e02f4437b89058a073dc7874aed8f"] == "ETH"

    eth = "Here is my 0x8145D05037d1778E232ACE2FaF9731a6E5b19538 Ethereum address."
    result = extract_cryptos(eth)
    assert "0x8145D05037d1778E232ACE2FaF9731a6E5b19538" in result
    assert result["0x8145D05037d1778E232ACE2FaF9731a6E5b19538"] == "ETH"

    # TRON
    tron = "hamas TXEsK1sEsKjZ1xtHitnyAAoqw3WLdYdRNW"
    result = extract_cryptos(tron)
    assert len(result) == 1
    assert "TXEsK1sEsKjZ1xtHitnyAAoqw3WLdYdRNW" in result
    assert result["TXEsK1sEsKjZ1xtHitnyAAoqw3WLdYdRNW"] == "TRON"

    # Bitcoin legacy
    btc = "Send to 18yzhmcgHtRVoEX3doCrqhis6fFU1dHFUE"
    result = extract_cryptos(btc)
    assert "18yzhmcgHtRVoEX3doCrqhis6fFU1dHFUE" in result
    assert result["18yzhmcgHtRVoEX3doCrqhis6fFU1dHFUE"] == "BTC"

    # Bitcoin bech32
    btc_bech = "SegWit: bc1qwsqdcas3llkcx53sx4lqrcrdpxmr5s4eke6d8y"
    result = extract_cryptos(btc_bech)
    assert "bc1qwsqdcas3llkcx53sx4lqrcrdpxmr5s4eke6d8y" in result
    assert result["bc1qwsqdcas3llkcx53sx4lqrcrdpxmr5s4eke6d8y"] == "BTC"

    # Litecoin
    ltc = "LTC: 3E6ZCKRrsdPc35chA9Eftp1h3DLW18NFNV"
    result = extract_cryptos(ltc)
    assert "3E6ZCKRrsdPc35chA9Eftp1h3DLW18NFNV" in result
    assert result["3E6ZCKRrsdPc35chA9Eftp1h3DLW18NFNV"] == "LTC"

    # Dash
    dash = "Dash: XyARKoupuArYtToA2S6yMdnoquDCDaBsaT"
    result = extract_cryptos(dash)
    assert "XyARKoupuArYtToA2S6yMdnoquDCDaBsaT" in result
    assert result["XyARKoupuArYtToA2S6yMdnoquDCDaBsaT"] == "DASH"

    # Monero
    xmr = "XMR: 49HqitRzdnhYjgTEAhgGpCfsjdTeMbUTU6cyR4JV1R7k2Eej9rGT8JpFiYDa4tZM6RZiFrHmMzgSrhHEqpDYKBe5B2ufNsL"
    result = extract_cryptos(xmr)
    assert (
        "49HqitRzdnhYjgTEAhgGpCfsjdTeMbUTU6cyR4JV1R7k2Eej9rGT8JpFiYDa4tZM6RZiFrHmMzgSrhHEqpDYKBe5B2ufNsL"
        in result
    )

    # Ripple
    xrp = "Send XRP: rnXyVQzgxZe7TR1EPzTkGj2jxH4LMJYh66"
    result = extract_cryptos(xrp)
    assert "rnXyVQzgxZe7TR1EPzTkGj2jxH4LMJYh66" in result
    assert result["rnXyVQzgxZe7TR1EPzTkGj2jxH4LMJYh66"] == "XRP"

    # Bitcoin Cash
    bch = "BCH: bitcoincash:qqyuc9s700plhzr6awzru7g5z2d2p906uyrm6ht0r0"
    result = extract_cryptos(bch)
    assert "bitcoincash:qqyuc9s700plhzr6awzru7g5z2d2p906uyrm6ht0r0" in result
    assert result["bitcoincash:qqyuc9s700plhzr6awzru7g5z2d2p906uyrm6ht0r0"] == "BCH"

    # Dogecoin
    doge = "Much wow: DNmxLVUn5AuzoDo2CSc7P13wcMSvZ4nsYY"
    result = extract_cryptos(doge)
    assert "DNmxLVUn5AuzoDo2CSc7P13wcMSvZ4nsYY" in result
    assert result["DNmxLVUn5AuzoDo2CSc7P13wcMSvZ4nsYY"] == "DOGE"

    # Should NOT extract partial LTC address from TRON address
    partial_bug = "TH96tFMn8KGiYSLiwcV3E2UiaJc8jmcbz3"
    result = extract_cryptos(partial_bug)
    assert "Mn8KGiYSLiwcV3E2UiaJc8jmcbz3" not in result
    assert "TH96tFMn8KGiYSLiwcV3E2UiaJc8jmcbz3" in result
    assert result["TH96tFMn8KGiYSLiwcV3E2UiaJc8jmcbz3"] == "TRON"

    # Multiple addresses
    multi = "BTC: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa ETH: 0x8145D05037d1778E232ACE2FaF9731a6E5b19538"
    result = extract_cryptos(multi)
    assert len(result) == 2
    assert result["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"] == "BTC"
    assert result["0x8145D05037d1778E232ACE2FaF9731a6E5b19538"] == "ETH"

    # Address with punctuation
    punct = "Send to: 0x5512d943ed1f7c8a43f3435c85f7ab68b30121b0, thanks!"
    result = extract_cryptos(punct)
    assert "0x5512d943ed1f7c8a43f3435c85f7ab68b30121b0" in result

    # Should NOT match partial addresses embedded in alphanumeric strings
    embedded = "prefix0x1234567890123456789012345678901234567890suffix"
    result = extract_cryptos(embedded)
    assert len(result) == 0
