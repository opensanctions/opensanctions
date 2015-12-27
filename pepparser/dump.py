import os
import json
import logging
import unicodecsv
from xlsxwriter import Workbook


log = logging.getLogger(__name__)

SOURCE_FIELDS = ['publisher', 'publisher_url', 'source', 'source_id']
SORT_ENITTY = ['uid', 'name', 'type', 'function', 'other_name', 'text']


def get_headers(rows):
    headers = set()
    for row in rows:
        headers.update(row.keys())

    if 'id' in headers:
        headers.remove('id')
    return headers


def csv_generate(file_name, rows):
    if not len(rows):
        return
    headers = get_headers(rows)
    if not len(headers):
        return

    log.info(' -> %s (%s rows)', file_name, len(rows))
    with open(file_name, 'w') as fh:
        writer = unicodecsv.writer(fh)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(h) for h in headers])


def json_generate(file_name, jsons):
    log.info(' -> %s', file_name)
    with open(file_name, 'w') as fh:
        json.dump({'entities': jsons}, fh)


def xlsx_generate(file_name, sheets):
    log.info(' -> %s', file_name)
    with open(file_name, 'w') as fh:
        workbook = Workbook(fh)
        header_format = workbook.add_format({
            'bold': True,
            'border': 1
        })

        for title, rows in sheets:
            if not len(rows):
                continue
            headers = get_headers(rows)
            if not len(headers):
                continue

            worksheet = workbook.add_worksheet(title)
            worksheet.strings_to_numbers = False
            worksheet.strings_to_urls = False
            worksheet.strings_to_formulas = False

            row_idx = 0
            headers = None
            for row in rows:
                if headers is None:
                    headers = row.keys()
                    for c, cell in enumerate(headers):
                        worksheet.write(row_idx, c, cell, header_format)
                    row_idx += 1

                col_idx = 0
                for cell in headers:
                    worksheet.write(row_idx, col_idx, row.get(cell))
                    col_idx += 1
                row_idx += 1

            worksheet.freeze_panes(1, 0)
        workbook.close()


def dump_query(manager, prefix, name, q):
    uids = set()
    entities = []
    jsons = []
    file_base = os.path.join(prefix, name)
    meta = {
        'csv': [{
            'label': 'Entities',
            'file': name + '.entities.csv'
        }],
        'xlsx': name + '.xlsx',
        'json': name + '.json'
    }

    for entity in manager._entities.find(**q):
        uids.add(entity.get('uid'))
        json_repr = entity.pop('json', None)
        if json_repr is not None:
            jsons.append(json.loads(json_repr))
        entities.append(entity)
    entities = sorted(entities, key=lambda e: e.get('name'))
    csv_generate(file_base + '.entities.csv', entities)

    sheets = [('Entities', entities)]
    tables = {
        ('Aliases', 'other_names'): manager._other_names,
        ('Documents', 'identities'): manager._identities,
        ('Addresses', 'addresses'): manager._addresses
    }
    for (label, section), table in tables.items():
        q = table.table.select().where(table.table.c.uid.in_(uids))
        rows = list(manager.engine.query(q))
        rows = sorted(rows, key=lambda r: r.get('name'))
        csv_generate('%s.%s.csv' % (file_base, section), rows)
        sheets.append((label, rows))
        meta['csv'].append({
            'label': label,
            'file': '%s.%s.csv' % (name, section)
        })

    xlsx_generate(file_base + '.xlsx', sheets)
    json_generate(file_base + '.json', jsons)
    return meta


def dump_db(manager, outdir):
    try:
        os.makedirs(outdir)
    except:
        pass

    log.info('Generating full dumps...')
    meta = {
        'data': dump_query(manager, outdir, "full", {}),
        'sources': []
    }
    for source in manager._entities.distinct(*SOURCE_FIELDS):
        source_id = source.get('source_id')
        if source_id is None:
            continue
        log.info('Generating: %s...', source.get('source'))
        q = {'source_id': source_id}
        source['data'] = dump_query(manager, outdir, source_id.lower(), q)
        meta['sources'].append(source)

    with open(os.path.join(outdir, 'metadata.json'), 'w') as fh:
        json.dump(meta, fh, indent=2)
