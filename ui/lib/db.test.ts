import { db, NewReview, updateExtractionEntry } from './db';


describe('updateExtractionEntry integration', () => {
  beforeEach(async () => {
    // Clean up the table and create if needed
    await db.schema.dropTable('review').ifExists().execute();
    await db.schema
      .createTable('review')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('key', 'text')
      .addColumn('dataset', 'text')
      .addColumn('extraction_schema', 'json')
      .addColumn('source_value', 'text')
      .addColumn('source_content_type', 'text')
      .addColumn('source_label', 'text')
      .addColumn('source_url', 'text')
      .addColumn('accepted', 'boolean')
      .addColumn('orig_extraction_data', 'json')
      .addColumn('extracted_data', 'json')
      .addColumn('last_seen_version', 'text')
      .addColumn('modified_at', 'text')
      .addColumn('modified_by', 'text')
      .addColumn('deleted_at', 'text')
      .execute();
  });
  afterEach(async () => {
    await db.schema.dropTable('review').ifExists().execute();
  });

  it('marks old row as deleted and inserts new row with updated values', async () => {
    const now = new Date().toISOString();
    const initial: NewReview = {
      key: 'k1',
      dataset: 'd1',
      extraction_schema: { foo: 'bar' },
      source_value: 'sv',
      source_content_type: 'ct',
      source_label: 'lbl',
      source_url: 'url',
      accepted: false,
      orig_extraction_data: { a: 1 },
      extracted_data: { b: 2 },
      last_seen_version: 'v1',
      modified_at: now,
      modified_by: 'tester',
      deleted_at: null,
    };
    // Insert initial row
    await db.insertInto('review').values(initial).execute();

    // Call updateExtractionEntry
    const newAccepted = true;
    const newExtracted = { b: 99 };
    await updateExtractionEntry({
      dataset: 'd1',
      key: 'k1',
      accepted: newAccepted,
      extractedData: newExtracted,
    });

    // Check old row is marked as deleted
    const oldRows = await db.selectFrom('review')
      .selectAll()
      .where('dataset', '=', 'd1')
      .where('key', '=', 'k1')
      .where('deleted_at', 'is not', null)
      .execute();
    expect(oldRows.length).toBe(1);
    expect(oldRows[0].accepted).toBe(false);
    expect(oldRows[0].extracted_data).toEqual({ b: 2 });

    // Check new row is inserted with updated values
    const newRows = await db.selectFrom('review')
      .selectAll()
      .where('dataset', '=', 'd1')
      .where('key', '=', 'k1')
      .where('deleted_at', 'is', null)
      .execute();
    expect(newRows.length).toBe(1);
    expect(newRows[0].accepted).toBe(true);
    expect(newRows[0].extracted_data).toEqual(newExtracted);
    // All other fields should match initial (except modified_at/by, deleted_at)
    expect(newRows[0].dataset).toBe(initial.dataset);
    expect(newRows[0].key).toBe(initial.key);
    expect(newRows[0].extraction_schema).toEqual(initial.extraction_schema);
    expect(newRows[0].orig_extraction_data).toEqual(initial.orig_extraction_data);
    expect(newRows[0].source_value).toBe(initial.source_value);
    expect(newRows[0].source_content_type).toBe(initial.source_content_type);
    expect(newRows[0].source_label).toBe(initial.source_label);
    expect(newRows[0].source_url).toBe(initial.source_url);
    expect(newRows[0].last_seen_version).toBe(initial.last_seen_version);
    expect(newRows[0].deleted_at).toBeNull();
    expect(newRows[0].modified_by).toBe('zavod ui user');
  });
});
