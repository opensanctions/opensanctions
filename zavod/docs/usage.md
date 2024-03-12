# Using the command-line tool

Once you've successfully [installed](install.md) zavod, you can use the built-in command-line tool to run parts of the system:

```bash
# Before everything else, flush away cached source data. If you don't 
# do this, you'll essentially work in developer mode where a local
# cached copy of the source data is used instead of fetching fresh
# files.
$ zavod clear datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# Crawl the ICIJ OffshoreLeaks database:
$ zavod crawl datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# You can also export a dataset without re-crawling the sources:
$ zavod export datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# You can publish a dataset to the archive:
$ zavod publish  --latest datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# Combine crawl, export and publish in one command:
$ zavod run --latest datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml
```

When you are developing a crawler, it can be handy to rerun the crawler a number
of times using the data source cache, then export the data rebuilding the intermediate
storage.

First run the crawler without `--clear` until you are ready to export:

```bash
$ zavod crawl ...
```

Then run the exporter with `--clear` to ensure the latest statements are included in the output:

```bash
$ zavod export --clear ...```