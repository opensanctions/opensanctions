// NOTE: You must install 'pg', 'sqlite3', and 'sqlite' as dependencies for this to work.
// npm install pg sqlite3 sqlite

export async function getDatasetStats() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error('DATABASE_URL not set');

  // Determine DB type
  const isPostgres = dbUrl.startsWith('postgres');
  const isSqlite = dbUrl.startsWith('sqlite');

  let results: { dataset: string; total: number; unaccepted: number }[] = [];

  if (isPostgres) {
    const { Client } = await import('pg');
    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    // Get all datasets and their latest version
    const versionRes = await client.query(`
      SELECT dataset, MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE deleted_at IS NULL
      GROUP BY dataset
    `);
    const datasetVersions = versionRes.rows;
    let resultsArr: { dataset: string; total: number; unaccepted: number }[] = [];
    for (const { dataset, latest_version } of datasetVersions) {
      if (!latest_version) continue;
      const res = await client.query(`
        SELECT COUNT(*) as total,
          SUM(CASE WHEN accepted = false THEN 1 ELSE 0 END) as unaccepted
        FROM extraction
        WHERE dataset = $1 AND deleted_at IS NULL AND last_seen_version = $2
      `, [dataset, latest_version]);
      resultsArr.push({
        dataset,
        total: Number(res.rows[0].total),
        unaccepted: Number(res.rows[0].unaccepted),
      });
    }
    results = resultsArr;
    await client.end();
  } else if (isSqlite) {
    const sqlite3 = await import('sqlite3');
    const { open } = await import('sqlite');
    const db = await open({ filename: dbUrl.replace('sqlite://', ''), driver: sqlite3.Database });
    // Get all datasets and their latest version
    const versionRows = await db.all(`
      SELECT dataset, MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE deleted_at IS NULL
      GROUP BY dataset
    `);
    let resultsArr: { dataset: string; total: number; unaccepted: number }[] = [];
    for (const { dataset, latest_version } of versionRows) {
      if (!latest_version) continue;
      const rows = await db.all(`
        SELECT COUNT(*) as total,
          SUM(CASE WHEN accepted = 0 THEN 1 ELSE 0 END) as unaccepted
        FROM extraction
        WHERE dataset = ? AND deleted_at IS NULL AND last_seen_version = ?
      `, [dataset, latest_version]);
      resultsArr.push({
        dataset,
        total: Number(rows[0].total),
        unaccepted: Number(rows[0].unaccepted),
      });
    }
    results = resultsArr;
    await db.close();
  } else {
    throw new Error('Unsupported database type');
  }

  return results;
}

export async function getExtractionEntries(dataset: string) {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error('DATABASE_URL not set');

  const isPostgres = dbUrl.startsWith('postgres');
  const isSqlite = dbUrl.startsWith('sqlite');

  let entries: any[] = [];

  if (isPostgres) {
    const { Client } = await import('pg');
    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    // Get the latest version for the dataset
    const versionRes = await client.query(`
      SELECT MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE dataset = $1 AND deleted_at IS NULL
    `, [dataset]);
    const latestVersion = versionRes.rows[0]?.latest_version;
    if (!latestVersion) {
      await client.end();
      return [];
    }
    const res = await client.query(`
      SELECT id, key, source_url, modified_at, modified_by, accepted, orig_extraction_data
      FROM extraction
      WHERE dataset = $1 AND deleted_at IS NULL AND last_seen_version = $2
      ORDER BY accepted ASC, modified_at DESC
    `, [dataset, latestVersion]);
    entries = res.rows.map((row: any) => ({
      id: row.id,
      key: row.key,
      source_url: Array.isArray(row.source_url) ? row.source_url[0] : row.source_url,
      modified_at: row.modified_at,
      modified_by: row.modified_by,
      accepted: row.accepted,
      raw_data_snippet: JSON.stringify(row.orig_extraction_data).slice(0, 100),
    }));
    await client.end();
  } else if (isSqlite) {
    const sqlite3 = await import('sqlite3');
    const { open } = await import('sqlite');
    const dbFile = dbUrl.startsWith('sqlite:///')
      ? dbUrl.replace('sqlite:///', '/')
      : dbUrl.replace('sqlite://', '');
    const db = await open({ filename: dbFile, driver: sqlite3.Database });
    // Get the latest version for the dataset
    const versionRow = await db.get(`
      SELECT MAX(last_seen_version) AS latest_version
      FROM extraction
      WHERE dataset = ? AND deleted_at IS NULL
    `, [dataset]);
    const latestVersion = versionRow?.latest_version;
    if (!latestVersion) {
      await db.close();
      return [];
    }
    const rows = await db.all(`
      SELECT id, key, source_url, modified_at, modified_by, accepted, orig_extraction_data
      FROM extraction
      WHERE dataset = ? AND deleted_at IS NULL AND last_seen_version = ?
      ORDER BY accepted ASC, modified_at DESC
    `, [dataset, latestVersion]);
    entries = rows.map((row: any) => ({
      id: row.id,
      key: row.key,
      source_url: Array.isArray(row.source_url) ? row.source_url[0] : row.source_url,
      modified_at: row.modified_at,
      modified_by: row.modified_by,
      accepted: row.accepted,
      raw_data_snippet: JSON.stringify(row.orig_extraction_data).slice(0, 100),
    }));
    await db.close();
  } else {
    throw new Error('Unsupported database type');
  }

  return entries;
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
