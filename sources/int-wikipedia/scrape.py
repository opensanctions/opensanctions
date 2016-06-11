import dataset
import re
import mwclient

site = mwclient.Site('en.wikipedia.org')
disam = re.compile('\(.*\)$')
engine = dataset.connect('sqlite:///data.sqlite')
pages_table = engine['data']
categories_table = engine['categories']

COLLECTIONS = {
    'uganda': ['Ugandan_politicians', 'Presidents_of_Uganda', 'Ugandan_rebels',
               'Speakers_of_the_Parliament_of_Uganda', 'National_Resistance_Movement_politicians',
               'Prime_Ministers_of_Uganda', 'Government_ministers_of_Uganda',
               'Political_office-holders_in_Uganda'],
    'mozambique': ['Presidents_of_Mozambique', 'Heads_of_state_of_Mozambique',
                   'FRELIMO_politicians', 'Mozambican_politicians_by_party',
                   'Government_ministers_of_Mozambique'],
    'southafrica': ['Members_of_the_National_Assembly_of_South_Africa',
                    'South_African_revolutionaries', 'South_African_people_by_political_party',
                    'South_African_people_by_political_orientation']
}


def get_pages(cat):
    for p in cat:
        print p
        if p.namespace == 0:
            yield p
        elif p.namespace == 14:
            for pp in get_pages(p):
                yield pp


def filter_page(page):
    if not page.page_title:
        return False
    if page.page_title.startswith('List of'):
        return False
    return True


def clean_title(title):
    title = disam.sub('', title)
    return title.strip()


def page_url(page):
    slug = page.normalize_title(page.name)
    return 'http://%s/wiki/%s' % (page.site.host, slug)


def scrape_category(collection, name):
    cat = site.categories.get(name)
    for page in get_pages(cat):
        if not filter_page(page):
            continue
        if pages_table.find_one(collection=collection,
                                category=name,
                                entity=page.page_title):
            continue

        data = {
            'collection': collection,
            'category': name,
            'entity': page.page_title,
            'entity_url': page_url(page)
        }
        categories = []
        for cat in page.categories():
            row = data.copy()
            row['category'] = cat.page_title
            row['category_url'] = page_url(cat)
            categories_table.upsert(row, ['entity', 'category'])
            categories.append(cat.page_title)

        data['categories'] = '|'.join(categories)
        aliases = [page.page_title]
        aliases.extend([t for (lang, t) in page.langlinks()])

        for bl in page.backlinks(redirect=True):
            if not bl.redirect:
                continue
            if bl.redirects_to().page_title == page.page_title:
                aliases.append(bl.page_title)

        seen = set()
        for alias in aliases:
            alias = clean_title(alias)
            alias_norm = alias.lower()
            if alias_norm in seen:
                continue
            seen.add(alias_norm)
            row = dict(data)
            row['label'] = alias
            pages_table.upsert(row, ['label', 'entity'])


if __name__ == '__main__':
    for collection, categories in COLLECTIONS.items():
        for category in categories:
            scrape_category(collection, category)
