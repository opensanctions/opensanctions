import {
  ColumnType,
  Dialect,
  Generated,
  Insertable,
  InsertType,
  JSONColumnType,
  Kysely,
  PostgresDialect,
  Selectable,
  SelectType,
  SqliteDialect,
  Updateable,
} from 'kysely'
import { Pool } from 'pg'
import Database from 'better-sqlite3'

export interface ReviewTable {
  id: Generated<number>
  key: string
  dataset: string
  extraction_schema: JSONColumnType<object, string, string>
  source_value: string
  source_content_type: string
  source_label: string
  source_url: string
  accepted: boolean
  orig_extraction_data: JSONColumnType<object, string, string>
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
    database: new Database({filename: dbFile})
    });
} else {
  throw new Error(`Unsupported database type ${dbUrl}`);
}
export const db = new Kysely<ReviewDatabase>({dialect})



export async function getDatasetStats() {
  // 1. Get all datasets and their latest version
  const datasetVersions = await db
    .selectFrom('review')
    .select([
      'dataset',
      db.fn.max('last_seen_version').as('latest_version'),
    ])
    .where('deleted_at', 'is', null)
    .groupBy('dataset')
    .execute();

  let resultsArr: { dataset: string; total: number; unaccepted: number }[] = [];
  for (const { dataset, latest_version } of datasetVersions) {
    if (!latest_version) continue;
    // For each dataset/version, count total and unaccepted
    const res = await db
      .selectFrom('review')
      .select([
        db.fn.count<number>('id').as('total'),
        db.fn.sum<number>('accepted').as('accepted_sum'),
      ])
      .where('dataset', '=', dataset)
      .where('deleted_at', 'is', null)
      .where('last_seen_version', '=', latest_version)
      .executeTakeFirst();
    // accepted_sum is the number of accepted rows (true=1, false=0), so unaccepted = total - accepted_sum
    const total = Number(res?.total ?? 0);
    const acceptedSum = Number(res?.accepted_sum ?? 0);
    resultsArr.push({
      dataset,
      total,
      unaccepted: total - acceptedSum,
    });
  }
  return resultsArr;
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
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error('DATABASE_URL not set');

  const isPostgres = dbUrl.startsWith('postgres');
  const isSqlite = dbUrl.startsWith('sqlite');

  let entry: any = null;

  if (isPostgres) {
    const { Client } = await import('pg');
    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    const res = await client.query(`
      SELECT id, key, source_url, modified_at, accepted, orig_extraction_data, schema, extracted_data, source_value, source_content_type, source_label
      FROM extraction
      WHERE dataset = $1 AND key = $2 AND deleted_at IS NULL
      ORDER BY modified_at DESC
      LIMIT 1
    `, [dataset, key]);
    if (res.rows.length > 0) {
      const row = res.rows[0];
      entry = {
        id: row.id,
        key: row.key,
        source_url: Array.isArray(row.source_url) ? row.source_url[0] : row.source_url,
        modified_at: row.modified_at,
        accepted: row.accepted,
        raw_data: row.orig_extraction_data,
        schema: row.schema,
        extracted_data: row.extracted_data,
        source_value: row.source_value,
        source_content_type: row.source_content_type,
        source_label: row.source_label,
      };
    }
    await client.end();
  } else if (isSqlite) {
    const sqlite3 = await import('sqlite3');
    const { open } = await import('sqlite');
    const dbFile = dbUrl.startsWith('sqlite:///')
      ? dbUrl.replace('sqlite:///', '/')
      : dbUrl.replace('sqlite://', '');
    const db = await open({ filename: dbFile, driver: sqlite3.Database });
    const rows = await db.all(`
      SELECT id, key, source_url, modified_at, accepted, orig_extraction_data, schema, extracted_data, source_value
      FROM extraction
      WHERE dataset = ? AND key = ? AND deleted_at IS NULL
      ORDER BY modified_at DESC
      LIMIT 1
    `, [dataset, key]);
    if (rows.length > 0) {
      const row = rows[0];
      entry = {
        id: row.id,
        key: row.key,
        source_url: Array.isArray(row.source_url) ? row.source_url[0] : row.source_url,
        modified_at: row.modified_at,
        accepted: row.accepted,
        raw_data: row.orig_extraction_data,
        schema: row.schema,
        extracted_data: row.extracted_data,
        source_value: row.source_value,
      };
    }
    await db.close();
  } else {
    throw new Error('Unsupported database type');
  }

  return entry;
}

export async function updateExtractionEntry({ dataset, key, accepted, extractedData }: { dataset: string, key: string, accepted: boolean, extractedData: string }) {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error('DATABASE_URL not set');
  const isPostgres = dbUrl.startsWith('postgres');
  const isSqlite = dbUrl.startsWith('sqlite');
  const now = new Date().toISOString();

  if (isPostgres) {
    const { Client } = await import('pg');
    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    // Mark current as deleted
    await client.query(
      `UPDATE extraction SET deleted_at = $1 WHERE dataset = $2 AND key = $3 AND deleted_at IS NULL`,
      [now, dataset, key]
    );
    // Insert new row copying fields, updating accepted/extracted_data/modified_at
    await client.query(
      `INSERT INTO extraction (dataset, last_seen_version, orig_extraction_data, orig_extraction_data_hash, key, source_url, schema, accepted, extracted_data, modified_at, modified_by, source_value, source_content_type, source_label)
       SELECT dataset, last_seen_version, orig_extraction_data, orig_extraction_data_hash, key, source_url, schema, $3, $4, $5, $6, source_value, source_content_type, source_label
       FROM extraction
       WHERE dataset = $1 AND key = $2
       ORDER BY modified_at DESC
       LIMIT 1`,
      [dataset, key, accepted, extractedData, now, 'zavod ui user']
    );
    await client.end();
  } else if (isSqlite) {
    const sqlite3 = await import('sqlite3');
    const { open } = await import('sqlite');
    const dbFile = dbUrl.startsWith('sqlite:///')
      ? dbUrl.replace('sqlite:///', '/')
      : dbUrl.replace('sqlite://', '');
    const db = await open({ filename: dbFile, driver: sqlite3.Database });
    await db.run(
      `UPDATE extraction SET deleted_at = ? WHERE dataset = ? AND key = ? AND deleted_at IS NULL`,
      [now, dataset, key]
    );
    await db.run(
      `INSERT INTO extraction (dataset, last_seen_version, orig_extraction_data, orig_extraction_data_hash, key, source_url, schema, accepted, extracted_data, modified_at, modified_by, source_value, source_content_type, source_label)
       SELECT dataset, last_seen_version, orig_extraction_data, orig_extraction_data_hash, key, source_url, schema, ?, ?, ?, ?, source_value, source_content_type, source_label
       FROM extraction
       WHERE dataset = ? AND key = ?
       ORDER BY modified_at DESC
       LIMIT 1`,
      [accepted, extractedData, now, 'zavod ui user', dataset, key]
    );
    await db.close();
  } else {
    throw new Error('Unsupported database type');
  }
}

export async function getNextUnacceptedEntryKey(dataset: string): Promise<string | null> {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error('DATABASE_URL not set');
  const isPostgres = dbUrl.startsWith('postgres');
  const isSqlite = dbUrl.startsWith('sqlite');
  let key: string | null = null;
  if (isPostgres) {
    const { Client } = await import('pg');
    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    // First get the latest version for this dataset
    const versionRes = await client.query(`
      SELECT MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE dataset = $1 AND deleted_at IS NULL
    `, [dataset]);
    const latestVersion = versionRes.rows[0]?.latest_version;
    if (latestVersion) {
      const res = await client.query(`
        SELECT key FROM extraction
        WHERE dataset = $1
        AND deleted_at IS NULL
        AND accepted = false
        AND last_seen_version = $2
        ORDER BY modified_at DESC
        LIMIT 1
      `, [dataset, latestVersion]);
      if (res.rows.length > 0) key = res.rows[0].key;
    }
    await client.end();
  } else if (isSqlite) {
    const sqlite3 = await import('sqlite3');
    const { open } = await import('sqlite');
    const dbFile = dbUrl.startsWith('sqlite:///')
      ? dbUrl.replace('sqlite:///', '/')
      : dbUrl.replace('sqlite://', '');
    const db = await open({ filename: dbFile, driver: sqlite3.Database });
    // First get the latest version for this dataset
    const versionRow = await db.get(`
      SELECT MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE dataset = ? AND deleted_at IS NULL
    `, [dataset]);
    const latestVersion = versionRow?.latest_version;
    if (latestVersion) {
      const rows = await db.all(`
        SELECT key FROM extraction
        WHERE dataset = ?
        AND deleted_at IS NULL
        AND accepted = 0
        AND last_seen_version = ?
        ORDER BY modified_at DESC
        LIMIT 1
      `, [dataset, latestVersion]);
      if (rows.length > 0) key = rows[0].key;
    }
    await db.close();
  } else {
    throw new Error('Unsupported database type');
  }
  return key;
}
