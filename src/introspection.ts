import { getKnex, getDialect, type Dialect } from './database.js';

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue: string | null;
  isPrimaryKey: boolean;
}

interface ForeignKey {
  column: string;
  referencedTable: string;
  referencedColumn: string;
}

interface IndexInfo {
  name: string;
  columns: string[];
  unique: boolean;
}

interface TableDescription {
  name: string;
  columns: ColumnInfo[];
  foreignKeys: ForeignKey[];
  indexes: IndexInfo[];
}

interface TableSummary {
  name: string;
  estimatedRows: number;
}

// ===== List Tables =====

async function listTablesPg(): Promise<TableSummary[]> {
  const knex = getKnex();
  const result = await knex.raw(`
    SELECT
      t.tablename AS name,
      COALESCE(c.reltuples, 0)::bigint AS estimated_rows
    FROM pg_tables t
    LEFT JOIN pg_class c ON c.relname = t.tablename
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
    WHERE t.schemaname = 'public'
    ORDER BY t.tablename
  `);
  return result.rows.map((r: any) => ({
    name: r.name,
    estimatedRows: Number(r.estimated_rows < 0 ? 0 : r.estimated_rows),
  }));
}

async function listTablesMysql(): Promise<TableSummary[]> {
  const knex = getKnex();
  const result = await knex.raw(`
    SELECT
      TABLE_NAME AS name,
      TABLE_ROWS AS estimated_rows
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
  `);
  const rows = result[0] || result;
  return rows.map((r: any) => ({
    name: r.name || r.TABLE_NAME,
    estimatedRows: Number(r.estimated_rows || r.TABLE_ROWS || 0),
  }));
}

async function listTablesSqlite(): Promise<TableSummary[]> {
  const knex = getKnex();
  const tables = await knex.raw(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
  );
  const results: TableSummary[] = [];
  for (const t of tables) {
    const count = await knex.raw(`SELECT COUNT(*) as cnt FROM "${t.name}"`);
    results.push({
      name: t.name,
      estimatedRows: count[0]?.cnt ?? 0,
    });
  }
  return results;
}

export async function listTables(): Promise<string> {
  const dialect = getDialect();
  let tables: TableSummary[];

  if (dialect === 'pg') tables = await listTablesPg();
  else if (dialect === 'mysql2') tables = await listTablesMysql();
  else tables = await listTablesSqlite();

  if (tables.length === 0) {
    return 'No tables found in the database.';
  }

  const maxNameLen = Math.max(...tables.map(t => t.name.length), 5);
  const header = `${'Table'.padEnd(maxNameLen)}  Est. Rows`;
  const separator = `${'─'.repeat(maxNameLen)}  ${'─'.repeat(10)}`;
  const rows = tables.map(t =>
    `${t.name.padEnd(maxNameLen)}  ${t.estimatedRows.toLocaleString()}`
  );

  return [header, separator, ...rows].join('\n');
}

// ===== Describe Table =====

async function describeTablePg(tableName: string): Promise<TableDescription> {
  const knex = getKnex();

  // Columns
  const cols = await knex.raw(`
    SELECT
      c.column_name AS name,
      c.data_type AS type,
      c.is_nullable = 'YES' AS nullable,
      c.column_default AS default_value,
      CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
    FROM information_schema.columns c
    LEFT JOIN (
      SELECT ku.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
      WHERE tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
    ) pk ON pk.column_name = c.column_name
    WHERE c.table_schema = 'public' AND c.table_name = ?
    ORDER BY c.ordinal_position
  `, [tableName, tableName]);

  // Foreign keys
  const fks = await knex.raw(`
    SELECT
      kcu.column_name AS "column",
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
    WHERE tc.table_name = ? AND tc.constraint_type = 'FOREIGN KEY'
  `, [tableName]);

  // Indexes
  const idxs = await knex.raw(`
    SELECT
      i.relname AS name,
      ix.indisunique AS "unique",
      array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE t.relname = ?
    GROUP BY i.relname, ix.indisunique
  `, [tableName]);

  return {
    name: tableName,
    columns: cols.rows.map((r: any) => ({
      name: r.name,
      type: r.type,
      nullable: r.nullable,
      defaultValue: r.default_value,
      isPrimaryKey: r.is_primary_key,
    })),
    foreignKeys: fks.rows.map((r: any) => ({
      column: r.column,
      referencedTable: r.referenced_table,
      referencedColumn: r.referenced_column,
    })),
    indexes: idxs.rows.map((r: any) => ({
      name: r.name,
      columns: r.columns,
      unique: r.unique,
    })),
  };
}

async function describeTableMysql(tableName: string): Promise<TableDescription> {
  const knex = getKnex();

  // Columns
  const cols = await knex.raw(`
    SELECT
      COLUMN_NAME AS name,
      COLUMN_TYPE AS type,
      IS_NULLABLE = 'YES' AS nullable,
      COLUMN_DEFAULT AS default_value,
      COLUMN_KEY = 'PRI' AS is_primary_key
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
  `, [tableName]);

  // Foreign keys
  const fks = await knex.raw(`
    SELECT
      COLUMN_NAME AS \`column\`,
      REFERENCED_TABLE_NAME AS referenced_table,
      REFERENCED_COLUMN_NAME AS referenced_column
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL
  `, [tableName]);

  // Indexes
  const idxs = await knex.raw(`
    SELECT
      INDEX_NAME AS name,
      NOT NON_UNIQUE AS \`unique\`,
      GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
    GROUP BY INDEX_NAME, NON_UNIQUE
  `, [tableName]);

  const colRows = cols[0] || cols;
  const fkRows = fks[0] || fks;
  const idxRows = idxs[0] || idxs;

  return {
    name: tableName,
    columns: colRows.map((r: any) => ({
      name: r.name || r.COLUMN_NAME,
      type: r.type || r.COLUMN_TYPE,
      nullable: Boolean(r.nullable || r.IS_NULLABLE === 'YES'),
      defaultValue: r.default_value ?? r.COLUMN_DEFAULT ?? null,
      isPrimaryKey: Boolean(r.is_primary_key || r.COLUMN_KEY === 'PRI'),
    })),
    foreignKeys: fkRows.map((r: any) => ({
      column: r.column || r.COLUMN_NAME,
      referencedTable: r.referenced_table || r.REFERENCED_TABLE_NAME,
      referencedColumn: r.referenced_column || r.REFERENCED_COLUMN_NAME,
    })),
    indexes: idxRows.map((r: any) => ({
      name: r.name || r.INDEX_NAME,
      columns: (r.columns || '').split(','),
      unique: Boolean(r.unique),
    })),
  };
}

async function describeTableSqlite(tableName: string): Promise<TableDescription> {
  const knex = getKnex();

  // Columns via PRAGMA
  const pragmaCols = await knex.raw(`PRAGMA table_info("${tableName}")`);
  const columns: ColumnInfo[] = pragmaCols.map((r: any) => ({
    name: r.name,
    type: r.type || 'TEXT',
    nullable: r.notnull === 0,
    defaultValue: r.dflt_value,
    isPrimaryKey: r.pk === 1,
  }));

  // Foreign keys
  const pragmaFks = await knex.raw(`PRAGMA foreign_key_list("${tableName}")`);
  const foreignKeys: ForeignKey[] = pragmaFks.map((r: any) => ({
    column: r.from,
    referencedTable: r.table,
    referencedColumn: r.to,
  }));

  // Indexes
  const pragmaIdxs = await knex.raw(`PRAGMA index_list("${tableName}")`);
  const indexes: IndexInfo[] = [];
  for (const idx of pragmaIdxs) {
    const idxInfo = await knex.raw(`PRAGMA index_info("${idx.name}")`);
    indexes.push({
      name: idx.name,
      columns: idxInfo.map((c: any) => c.name),
      unique: idx.unique === 1,
    });
  }

  return { name: tableName, columns, foreignKeys, indexes };
}

export async function describeTable(tableName: string): Promise<string> {
  const dialect = getDialect();
  let desc: TableDescription;

  if (dialect === 'pg') desc = await describeTablePg(tableName);
  else if (dialect === 'mysql2') desc = await describeTableMysql(tableName);
  else desc = await describeTableSqlite(tableName);

  const lines: string[] = [`Table: ${desc.name}`, ''];

  // Columns
  lines.push('Columns:');
  const maxColName = Math.max(...desc.columns.map(c => c.name.length), 4);
  const maxType = Math.max(...desc.columns.map(c => c.type.length), 4);
  for (const col of desc.columns) {
    const flags: string[] = [];
    if (col.isPrimaryKey) flags.push('PK');
    if (!col.nullable) flags.push('NOT NULL');
    if (col.defaultValue) flags.push(`DEFAULT ${col.defaultValue}`);
    const flagStr = flags.length ? `  [${flags.join(', ')}]` : '';
    lines.push(`  ${col.name.padEnd(maxColName)}  ${col.type.padEnd(maxType)}${flagStr}`);
  }

  // Foreign keys
  if (desc.foreignKeys.length > 0) {
    lines.push('');
    lines.push('Foreign Keys:');
    for (const fk of desc.foreignKeys) {
      lines.push(`  ${fk.column} → ${fk.referencedTable}.${fk.referencedColumn}`);
    }
  }

  // Indexes
  if (desc.indexes.length > 0) {
    lines.push('');
    lines.push('Indexes:');
    for (const idx of desc.indexes) {
      const uniq = idx.unique ? ' (UNIQUE)' : '';
      lines.push(`  ${idx.name}: (${idx.columns.join(', ')})${uniq}`);
    }
  }

  return lines.join('\n');
}

// ===== Full Schema =====

export async function getFullSchema(): Promise<string> {
  const dialect = getDialect();
  let tables: TableSummary[];

  if (dialect === 'pg') tables = await listTablesPg();
  else if (dialect === 'mysql2') tables = await listTablesMysql();
  else tables = await listTablesSqlite();

  if (tables.length === 0) {
    return 'No tables found in the database.';
  }

  if (tables.length > 100) {
    tables = tables.slice(0, 100);
  }

  const sections: string[] = [];
  sections.push(`Database Schema (${tables.length} tables)\n${'═'.repeat(40)}\n`);

  for (const t of tables) {
    const desc = await describeTable(t.name);
    sections.push(desc);
    sections.push(`  Estimated rows: ${t.estimatedRows.toLocaleString()}`);
    sections.push('');
  }

  return sections.join('\n');
}
