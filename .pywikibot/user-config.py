from zavod import settings as _settings

usernames["wikidata"]["test"] = \
    usernames["wikidata"]["wikidata"] = _settings.WD_USER

authenticate["*.wikidata.org"] = (
    _settings.WD_CONSUMER_TOKEN,
    _settings.WD_CONSUMER_SECRET,
    _settings.WD_ACCESS_TOKEN,
    _settings.WD_ACCESS_SECRET,
)