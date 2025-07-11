import {
  Dialect,
  Generated,
  Insertable,
  JSONColumnType,
  Kysely,
  PostgresDialect,
  Selectable,
  SqliteDialect,
  Updateable,
  sql,
} from 'kysely'
import { Pool } from 'pg'
import Database from 'better-sqlite3'

export interface ReviewTable {
  id: Generated<number>
  key: string
  dataset: string
  extraction_schema: JSONColumnType<object, object, object>
  source_value: string
  source_content_type: string
  source_label: string
  source_url: string | null
  accepted: boolean
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


const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) throw new Error('DATABASE_URL not set');
const isPostgres = dbUrl.startsWith('postgres');
const isSqlite = dbUrl.startsWith('sqlite');
let dialect: Dialect
if (isPostgres) {
  dialect = new PostgresDialect({
    pool: new Pool({
      connectionString: dbUrl,
      max: 10,
    }),
  })
} else if (isSqlite) {
  const dbFile = dbUrl.startsWith('sqlite:///')
      ? dbUrl.replace('sqlite:///', '/')
      : dbUrl.replace('sqlite://', '');
  dialect = new SqliteDialect({
    database: new Database(dbFile)
  });
} else {
  throw new Error(`Unsupported database type ${dbUrl}`);
}
export const db = new Kysely<ReviewDatabase>({dialect, log: ['query', 'error']})



export async function getDatasetStats() {
  // Subquery to get the latest version for each dataset
  const latestVersionSubquery = db
    .selectFrom('review')
    .select([
      'dataset',
      db.fn.max('last_seen_version').as('latest_version'),
    ])
    .where('deleted_at', 'is', null)
    .groupBy('dataset')
    .as('lv');

  // Join review with the subquery on dataset and last_seen_version, then aggregate
  const rows = await db
    .selectFrom('review')
    .innerJoin(latestVersionSubquery, join =>
      join.onRef('review.dataset', '=', 'lv.dataset')
          .onRef('review.last_seen_version', '=', 'lv.latest_version')
    )
    .select([
      'review.dataset',
      db.fn.count<number>('review.id').as('total'),
      db.fn.sum<number>(sql<number>`CAST(review.accepted AS integer)`).as('accepted_sum'),
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
  const latestVersionSubquery = db
    .selectFrom('review')
    .select(db.fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version
  const rows = await db
    .selectFrom('review')
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
    .orderBy('review.modified_at', 'desc')
    .execute();
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
  return await db
    .selectFrom('review')
    .where('dataset', '=', dataset)
    .where('key', '=', key)
    .where('deleted_at', 'is', null)
    .selectAll()
    .executeTakeFirst();
}

export async function updateExtractionEntry({ dataset, key, accepted, extractedData }: { dataset: string, key: string, accepted: boolean, extractedData: object }) {
  const now = new Date().toISOString();
  await db.transaction().execute(async (trx) => {
    // Mark current as deleted
    await trx
      .updateTable('review')
      .set({ deleted_at: now })
      .where('dataset', '=', dataset)
      .where('key', '=', key)
      .where('deleted_at', 'is', null)
      .execute();

    // Get the latest previous row for this dataset/key
    const prev = await trx
      .selectFrom('review')
      .selectAll()
      .where('dataset', '=', dataset)
      .where('key', '=', key)
      .executeTakeFirst();
    if (!prev) return;

    // Insert new row, copying fields but updating accepted, extractedData, modified_at, modified_by
    const { id, ...rest } = prev;
    const newRow: NewReview = {
      ...rest,
      extracted_data: extractedData,
      accepted: accepted,
      modified_at: now,
      modified_by: 'zavod ui user',
      deleted_at: null,
    };
    console.log(newRow);
    await trx
      .insertInto('review')
      .values(newRow)
      .execute();
  });
}

export async function getNextUnacceptedEntryKey(dataset: string): Promise<string | null> {
  // Subquery to get the latest version for the dataset
  const latestVersionSubquery = db
    .selectFrom('review')
    .select(db.fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version and filter for unaccepted
  const row = await db
    .selectFrom('review')
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
