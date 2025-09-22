import {
  Dialect,
  Generated,
  Insertable,
  JSONColumnType,
  Kysely,
  PostgresDialect,
  Selectable,
  Updateable,
  sql,
} from 'kysely'
import { Pool } from 'pg'

import { DATABASE_URI } from './constants';

// Types are compile time. We want some sanity checking on the schema at runtime
const expectedColumns = new Set<string>([
  'id',
  'key',
  'dataset',
  'extraction_checksum',
  'extraction_schema',
  'source_value',
  'source_mime_type',
  'source_label',
  'source_url',
  'accepted',
  'crawler_version',
  'orig_extraction_data',
  'extracted_data',
  'last_seen_version',
  'modified_at',
  'modified_by',
  'deleted_at',
])
const existingColumnNames = new Set<string>();
export interface ReviewTable {
  id: Generated<number>
  key: string
  dataset: string
  extraction_schema: JSONColumnType<object, object, object>
  source_value: string
  source_mime_type: string
  source_label: string
  source_url: string | null
  accepted: boolean
  crawler_version: number
  orig_extraction_data: JSONColumnType<object, object, object>
  extracted_data: JSONColumnType<object, object, object>
  last_seen_version: string
  modified_at: string
  modified_by: string
  deleted_at: string | null
}
export type Review = Selectable<ReviewTable>
export type NewReview = Insertable<ReviewTable>
export type ReviewUpdate = Updateable<ReviewTable>

export interface ReviewDatabase {
  review: ReviewTable
}

const tableName = 'review';
const dbUrl = DATABASE_URI;
let db: Kysely<ReviewDatabase> | null = null;

export async function getDb(assertSchema: boolean = true) {
  if (!db) {
    if (!dbUrl) {
      throw new Error('ZAVOD_DATABASE_URI not set');
    }
    const isPostgres = dbUrl.startsWith('postgres');
    let dialect: Dialect
    if (isPostgres) {
      dialect = new PostgresDialect({
        pool: new Pool({
          connectionString: dbUrl,
          max: 10,
        }),
      })
    } else {
      throw new Error(`Unsupported database type ${dbUrl}`);
    }
    const uncheckedDb = new Kysely<ReviewDatabase>({ dialect, log: ['error'] })
    if (assertSchema) {
      await assertSchemaMatchesExpected(uncheckedDb);
    }
    db = uncheckedDb;
  }
  return db;
}

async function assertSchemaMatchesExpected(db: Kysely<ReviewDatabase>) {
  // Check that the table schema matches the expected schema
  const tables = await db.introspection.getTables()
  const reviewTable = tables.find(t => t.name === tableName)
  if (!reviewTable) {
    throw new Error(`Review table not found in database`);
  }
  for (const column of reviewTable.columns) {
    existingColumnNames.add(column.name);
  }
  const unexpected = expectedColumns.difference(existingColumnNames);
  const missing = existingColumnNames.difference(expectedColumns);
  if (unexpected.size > 0 || missing.size > 0) {
    let msg = `Table ${tableName} doesn't match expected schema. `;
    if (unexpected.size > 0)
      msg += `Unexpected columns: ${Array.from(unexpected).join(', ')}. `;
    if (missing.size > 0)
      msg += `Missing columns: ${Array.from(missing).join(', ')}. `;
    throw new Error(msg);
  }
}

export interface IDatasetStats {
  dataset: string;
  total: number;
  unaccepted: number;
}

export async function getDatasetStats(): Promise<IDatasetStats[]> {
  // Subquery to get the latest version for each dataset
  const latestVersionSubquery = (await getDb())
    .selectFrom(tableName)
    .select([
      'dataset',
      (await getDb()).fn.max('last_seen_version').as('latest_version'),
    ])
    .where('deleted_at', 'is', null)
    .groupBy('dataset')
    .as('lv');

  // Join review with the subquery on dataset and last_seen_version, then aggregate
  const rows = await (await getDb())
    .selectFrom(tableName)
    .innerJoin(latestVersionSubquery, join =>
      join.onRef('review.dataset', '=', 'lv.dataset')
        .onRef('review.last_seen_version', '=', 'lv.latest_version')
    )
    .select([
      'review.dataset',
      (await getDb()).fn.count<number>('review.id').as('total'),
      (await getDb()).fn.sum<number>(sql<number>`CAST(review.accepted AS integer)`).as('accepted_sum'),
    ])
    .where('review.deleted_at', 'is', null)
    .groupBy('review.dataset')
    .execute();

  return rows.map(row => ({
    dataset: row.dataset,
    total: Number(row.total ?? 0),
    unaccepted: Number(row.total ?? 0) - Number(row.accepted_sum ?? 0),
  }));
}

export async function getExtractionEntries(dataset: string) {
  // Subquery to get the latest version for the dataset
  const latestVersionSubquery = (await getDb())
    .selectFrom(tableName)
    .select((await getDb()).fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version
  const rows = await (await getDb())
    .selectFrom(tableName)
    .innerJoin(latestVersionSubquery, 'review.last_seen_version', 'lv.latest_version')
    .select([
      'review.id',
      'review.key',
      'review.source_url',
      'review.modified_at',
      'review.modified_by',
      'review.accepted',
      'review.orig_extraction_data',
    ])
    .where('review.dataset', '=', dataset)
    .where('review.deleted_at', 'is', null)
    .orderBy('review.accepted', 'asc')
    .orderBy('review.modified_at', 'asc')
    .execute();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return rows.map((row: any) => ({
    id: row.id,
    key: row.key,
    source_url: Array.isArray(row.source_url) ? row.source_url[0] : row.source_url,
    modified_at: row.modified_at,
    modified_by: row.modified_by,
    accepted: row.accepted,
    raw_data_snippet: JSON.stringify(row.orig_extraction_data).slice(0, 100),
  }));
}

export async function getExtractionEntry(dataset: string, key: string) {
  // Use Kysely to fetch the latest entry for the given dataset and key
  return await (await getDb())
    .selectFrom(tableName)
    .where('dataset', '=', dataset)
    .where('key', '=', key)
    .where('deleted_at', 'is', null)
    .selectAll()
    .executeTakeFirst();
}

export async function updateExtractionEntry({ dataset, key, accepted, extractedData, modifiedBy }: { dataset: string, key: string, accepted: boolean, extractedData: object, modifiedBy: string }) {
  const now = new Date().toUTCString();
  await (await getDb()).transaction().execute(async (trx) => {
    // Get the current non-deleted row
    const prev = await trx
      .selectFrom(tableName)
      .selectAll()
      .where('dataset', '=', dataset)
      .where('key', '=', key)
      .where('deleted_at', 'is', null)
      .executeTakeFirst();
    if (!prev) return;

    // Mark that specific row as deleted
    await trx
      .updateTable(tableName)
      .set({ deleted_at: now })
      .where('id', '=', prev.id)
      .execute();

    // Insert new row, copying fields but updating accepted, extractedData, modified_at, modified_by
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { id, ...rest } = prev;
    const newRow: NewReview = {
      ...rest,
      extracted_data: extractedData,
      accepted: accepted,
      modified_at: now,
      modified_by: modifiedBy,
      deleted_at: null,
    };
    await trx
      .insertInto(tableName)
      .values(newRow)
      .execute();
  });
}

export async function getNextUnacceptedEntryKey(dataset: string): Promise<string | null> {
  // Subquery to get the latest version for the dataset
  const latestVersionSubquery = (await getDb())
    .selectFrom(tableName)
    .select((await getDb()).fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version and filter for unaccepted
  const row = await (await getDb())
    .selectFrom(tableName)
    .innerJoin(latestVersionSubquery, 'review.last_seen_version', 'lv.latest_version')
    .select('review.key')
    .where('review.dataset', '=', dataset)
    .where('review.deleted_at', 'is', null)
    .where('review.accepted', '=', false)
    .orderBy('review.modified_at', 'desc')
    .limit(1)
    .executeTakeFirst();
  return row?.key ?? null;
}
