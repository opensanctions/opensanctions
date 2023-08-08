# Using the command-line tool

Once you've successfully [installed][install.md] zavod, you can use the built-in command-line tool to run parts of the system:

```bash
# Before everything else, flush away cached source data. If you don't 
# do this, you'll essentially work in developer mode where a local
# cached copy of the source data is used instead of fetching fresh
# files:
$ opensanctions clear-workdir

# Crawl and export the US consolidated list:
$ zavod run datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml
$ zavod export datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# If you're developing the crawler, you can skip generating the exports and
# run the code in dry-run mode which does not store the results:
$ zavod run datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# You can also export a dataset without re-crawling the sources:
$ zavod export datasets/_global/icij_offshoreleaks/icij_offshoreleaks.yml

# During development you might also want to force delete all data linked
# to a source:
$ opensanctions clear us_ofac_cons
```
