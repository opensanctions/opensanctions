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
  'extraction_schema',
  'source_value',
  'source_mime_type',
  'source_label',
  'source_url',
  'accepted',
  'model_version',
  'orig_extraction_data',
  'extracted_data',
  'last_seen_version',
  'modified_at',
  'modified_by',
  'deleted_at',
])

const expectedPositionColumns = new Set<string>([
  'id',
  'entity_id',
  'caption',
  'countries',
  'is_pep',
  'topics',
  'dataset',
  'created_at',
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
  model_version: number
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

export interface PositionTable {
  id: Generated<number>
  entity_id: string
  caption: string
  countries: JSONColumnType<string[], string[], string[]>
  is_pep: boolean | null
  topics: JSONColumnType<string[], string[], string[]>
  dataset: string
  created_at: Date
  modified_at: Date | null
  modified_by: string | null
  deleted_at: Date | null
}
export type Position = Selectable<PositionTable>
export type NewPosition = Insertable<PositionTable>
export type PositionUpdate = Updateable<PositionTable>

export interface ReviewDatabase {
  review: ReviewTable
  position: PositionTable
}

const REVIEW_TABLE_NAME = 'review';
const POSITION_TABLE_NAME = 'position';
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
    const uncheckedDb = new Kysely<ReviewDatabase>({ dialect, log: ['error', 'query'] })
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

  // Check review table
  const reviewTable = tables.find(t => t.name === REVIEW_TABLE_NAME)
  if (!reviewTable) {
    throw new Error(`Review table not found in database`);
  }
  for (const column of reviewTable.columns) {
    existingColumnNames.add(column.name);
  }
  const unexpected = expectedColumns.difference(existingColumnNames);
  const missing = existingColumnNames.difference(expectedColumns);
  if (unexpected.size > 0 || missing.size > 0) {
    let msg = `Table ${REVIEW_TABLE_NAME} doesn't match expected schema. `;
    if (unexpected.size > 0)
      msg += `Unexpected columns: ${Array.from(unexpected).join(', ')}. `;
    if (missing.size > 0)
      msg += `Missing columns: ${Array.from(missing).join(', ')}. `;
    throw new Error(msg);
  }

  // Check position table
  const positionTable = tables.find(t => t.name === POSITION_TABLE_NAME)
  if (!positionTable) {
    throw new Error(`Position table not found in database`);
  }
  const existingPositionColumnNames = new Set<string>();
  for (const column of positionTable.columns) {
    existingPositionColumnNames.add(column.name);
  }
  const unexpectedPosition = expectedPositionColumns.difference(existingPositionColumnNames);
  const missingPosition = existingPositionColumnNames.difference(expectedPositionColumns);
  if (unexpectedPosition.size > 0 || missingPosition.size > 0) {
    let msg = `Table position doesn't match expected schema. `;
    if (unexpectedPosition.size > 0)
      msg += `Unexpected columns: ${Array.from(unexpectedPosition).join(', ')}. `;
    if (missingPosition.size > 0)
      msg += `Missing columns: ${Array.from(missingPosition).join(', ')}. `;
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
    .selectFrom(REVIEW_TABLE_NAME)
    .select([
      'dataset',
      (await getDb()).fn.max('last_seen_version').as('latest_version'),
    ])
    .where('deleted_at', 'is', null)
    .groupBy('dataset')
    .as('lv');

  // Join review with the subquery on dataset and last_seen_version, then aggregate
  const rows = await (await getDb())
    .selectFrom(REVIEW_TABLE_NAME)
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
    .selectFrom(REVIEW_TABLE_NAME)
    .select((await getDb()).fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version
  const rows = await (await getDb())
    .selectFrom(REVIEW_TABLE_NAME)
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
    .selectFrom(REVIEW_TABLE_NAME)
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
      .selectFrom(REVIEW_TABLE_NAME)
      .selectAll()
      .where('dataset', '=', dataset)
      .where('key', '=', key)
      .where('deleted_at', 'is', null)
      .executeTakeFirst();
    if (!prev) return;

    // Mark that specific row as deleted
    await trx
      .updateTable(REVIEW_TABLE_NAME)
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
      .insertInto(REVIEW_TABLE_NAME)
      .values(newRow)
      .execute();
  });
}

export async function getNextUnacceptedEntryKey(dataset: string): Promise<string | null> {
  // Subquery to get the latest version for the dataset
  const latestVersionSubquery = (await getDb())
    .selectFrom(REVIEW_TABLE_NAME)
    .select((await getDb()).fn.max('last_seen_version').as('latest_version'))
    .where('dataset', '=', dataset)
    .where('deleted_at', 'is', null)
    .as('lv');

  // Join review with the subquery on last_seen_version and filter for unaccepted
  const row = await (await getDb())
    .selectFrom(REVIEW_TABLE_NAME)
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

export interface IPositionFilters {
  dataset?: string;
  entity_id?: string;
  caption?: string;
  country?: string;
  is_pep?: boolean | null;
  q?: string;
  sort?: string[];
  limit?: number;
  offset?: number;
}

export interface IPositionListResponse {
  limit: number;
  offset: number;
  total: number;
  results: Position[];
}

export async function getPositionsDatasets(): Promise<string[]> {
  const db = await getDb();

  const rows = await db
    .selectFrom(POSITION_TABLE_NAME)
    .select('dataset')
    .where('deleted_at', 'is', null)
    .distinct()
    .orderBy('dataset', 'asc')
    .execute();

  return rows.map(row => row.dataset);
}

export async function getPositions(filters: IPositionFilters = {}): Promise<IPositionListResponse> {
  const {
    dataset,
    entity_id,
    caption,
    country,
    is_pep,
    q,
    limit = 50,
    offset = 0,
  } = filters;

  const db = await getDb();

  // Build the base query
  let query = db
    .selectFrom(POSITION_TABLE_NAME)
    .where('deleted_at', 'is', null);

  if (entity_id !== undefined) {
    query = query.where('entity_id', '=', entity_id);
  }
  if (caption !== undefined) {
    query = query.where('caption', '=', caption);
  }
  if (country !== undefined) {
    // TODO(Leon Handreke): How safe is this? I don't know.
    query = query.where(sql`countries::text`, 'like', sql`'%' || ${country} || '%'`);
  }
  if (dataset !== undefined) {
    query = query.where('dataset', '=', dataset);
  }
  if (q !== undefined) {
    query = query.where('caption', 'ilike', `%${q}%`);
  }
  if (is_pep !== undefined) {
    if (is_pep === null) {
      query = query.where('is_pep', 'is', null);
    } else {
      query = query.where('is_pep', '=', is_pep);
    }
  }

  // Get total count
  const countQuery = query.select(db.fn.countAll().as('count'));
  const { count } = await countQuery.executeTakeFirstOrThrow();
  const total = Number(count);

  // Apply limit and offset
  query = query.limit(limit).offset(offset);

  // Execute the query
  const results = await query.selectAll().orderBy('created_at', 'desc').execute();

  return {
    limit,
    offset,
    total,
    results,
  };
}

export async function softDeleteAndCreatePosition({ entityId, positionUpdate, modifiedBy } : { entityId: string, positionUpdate: PositionUpdate, modifiedBy: string }): Promise<Position> {
  return await (await getDb()).transaction().execute(async (trx) => {
    // Assert that positionUpdate does not contain modified_at or modified_by
    if ('modified_at' in positionUpdate || 'modified_by' in positionUpdate) {
      throw new Error('positionUpdate cannot contain modified_at or modified_by fields');
    }

    const now = new Date();

    // First get the current position by positionId
    const currentPosition = await trx
      .selectFrom(POSITION_TABLE_NAME)
      .selectAll()
      .where('entity_id', '=', entityId)
      .where('deleted_at', 'is', null)
      .executeTakeFirst();

    if (!currentPosition) {
      throw new Error(`Position with entity_id ${entityId} not found or marked as deleted`);
    }

    // Mark the current position as deleted
    await trx
      .updateTable(POSITION_TABLE_NAME)
      .set({ deleted_at: now })
      .where('id', '=', currentPosition.id)
      .execute();

    // Drop id, modified_by, modified_at, deleted_at from the data
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { id, modified_by, modified_at, deleted_at, ...baseData } = currentPosition;

    // Apply positionUpdate to the data
    const updatedData = {
      ...baseData,
      ...positionUpdate,
      modified_at: now,
      modified_by: modifiedBy,
    };

    // Do the insertion
    const result = await trx
      .insertInto(POSITION_TABLE_NAME)
      .values({
        ...updatedData,
        // NOTE(Leon Handreke): I don't know how else to make it work than to stringify
        // the lists manually.
        countries: JSON.stringify(updatedData.countries) as unknown as string[],
        topics: JSON.stringify(updatedData.topics) as unknown as string[],
      })
      .returningAll()
      .executeTakeFirst();

    if (!result) {
      throw new Error(`Failed to create updated position for entity_id ${entityId}`);
    }

    return result;
  });
}
