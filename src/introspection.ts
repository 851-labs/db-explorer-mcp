import { getDialect, query, type Dialect } from './database.js';

function dialectName(d: Dialect): string {
  return d === 'pg' ? 'PostgreSQL' : d === 'mysql2' ? 'MySQL' : 'SQLite';
}

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
  type: string | null;
  isPrimary: boolean;
  cardinality: number | null;
  partial: string | null;
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
  const rows = await query<{ name: string; estimated_rows: string }>(`
    SELECT
      t.tablename AS name,
      COALESCE(c.reltuples, 0)::bigint AS estimated_rows
    FROM pg_tables t
    LEFT JOIN pg_class c ON c.relname = t.tablename
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
    WHERE t.schemaname = 'public'
    ORDER BY t.tablename
  `);
  return rows.map((r) => ({
    name: r.name,
    estimatedRows: Number(Number(r.estimated_rows) < 0 ? 0 : r.estimated_rows),
  }));
}

async function listTablesMysql(): Promise<TableSummary[]> {
  const rows = await query<{ name: string; estimated_rows: number }>(`
    SELECT
      TABLE_NAME AS name,
      TABLE_ROWS AS estimated_rows
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
  `);
  return rows.map((r) => ({
    name: r.name,
    estimatedRows: Number(r.estimated_rows || 0),
  }));
}

async function listTablesSqlite(): Promise<TableSummary[]> {
  const tables = await query<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
  );
  const results: TableSummary[] = [];
  for (const t of tables) {
    const count = await query<{ cnt: number }>(`SELECT COUNT(*) as cnt FROM "${t.name}"`);
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

  return [`Database: ${dialectName(dialect)}`, '', header, separator, ...rows].join('\n');
}

// ===== Describe Table =====

async function describeTablePg(tableName: string): Promise<TableDescription> {
  // Columns
  const cols = await query<{ name: string; type: string; nullable: boolean; default_value: string | null; is_primary_key: boolean }>(`
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
  const fks = await query<{ column: string; referenced_table: string; referenced_column: string }>(`
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
  const idxs = await query<{ name: string; unique: boolean; isPrimary: boolean; type: string; columns: string[]; partial: string | null; cardinality: string | null }>(`
    SELECT
      i.relname AS name,
      ix.indisunique AS "unique",
      ix.indisprimary AS "isPrimary",
      am.amname AS type,
      array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns,
      pg_get_expr(ix.indpred, ix.indrelid) AS partial,
      ic.reltuples::bigint AS cardinality
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_class ic ON ic.oid = ix.indexrelid
    JOIN pg_am am ON am.oid = i.relam
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE t.relname = ?
    GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname,
             ix.indpred, ix.indrelid, ic.reltuples
  `, [tableName]);

  return {
    name: tableName,
    columns: cols.map((r) => ({
      name: r.name,
      type: r.type,
      nullable: r.nullable,
      defaultValue: r.default_value,
      isPrimaryKey: r.is_primary_key,
    })),
    foreignKeys: fks.map((r) => ({
      column: r.column,
      referencedTable: r.referenced_table,
      referencedColumn: r.referenced_column,
    })),
    indexes: idxs.map((r) => ({
      name: r.name,
      columns: r.columns,
      unique: Boolean(r.unique),
      type: r.type,
      isPrimary: Boolean(r.isPrimary),
      cardinality: r.cardinality != null ? Number(r.cardinality) : null,
      partial: r.partial ?? null,
    })),
  };
}

async function describeTableMysql(tableName: string): Promise<TableDescription> {
  // Columns
  const colRows = await query<{ name: string; type: string; nullable: number; default_value: string | null; is_primary_key: number }>(`
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
  const fkRows = await query<{ column: string; referenced_table: string; referenced_column: string }>(`
    SELECT
      COLUMN_NAME AS \`column\`,
      REFERENCED_TABLE_NAME AS referenced_table,
      REFERENCED_COLUMN_NAME AS referenced_column
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL
  `, [tableName]);

  // Indexes
  const idxRows = await query<{ name: string; unique: number; isPrimary: number; type: string; columns: string; cardinality: number | null }>(`
    SELECT
      INDEX_NAME AS name,
      NOT NON_UNIQUE AS \`unique\`,
      INDEX_NAME = 'PRIMARY' AS isPrimary,
      INDEX_TYPE AS type,
      GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
      MAX(CARDINALITY) AS cardinality
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
    GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
  `, [tableName]);

  return {
    name: tableName,
    columns: colRows.map((r) => ({
      name: r.name,
      type: r.type,
      nullable: Boolean(r.nullable),
      defaultValue: r.default_value ?? null,
      isPrimaryKey: Boolean(r.is_primary_key),
    })),
    foreignKeys: fkRows.map((r) => ({
      column: r.column,
      referencedTable: r.referenced_table,
      referencedColumn: r.referenced_column,
    })),
    indexes: idxRows.map((r) => ({
      name: r.name,
      columns: (r.columns || '').split(','),
      unique: Boolean(r.unique),
      type: r.type,
      isPrimary: Boolean(r.isPrimary),
      cardinality: r.cardinality != null ? Number(r.cardinality) : null,
      partial: null,
    })),
  };
}

async function describeTableSqlite(tableName: string): Promise<TableDescription> {
  // Columns via PRAGMA
  const pragmaCols = await query<{ name: string; type: string; notnull: number; dflt_value: string | null; pk: number }>(`PRAGMA table_info("${tableName}")`);
  const columns: ColumnInfo[] = pragmaCols.map((r) => ({
    name: r.name,
    type: r.type || 'TEXT',
    nullable: r.notnull === 0,
    defaultValue: r.dflt_value,
    isPrimaryKey: r.pk === 1,
  }));

  // Foreign keys
  const pragmaFks = await query<{ from: string; table: string; to: string }>(`PRAGMA foreign_key_list("${tableName}")`);
  const foreignKeys: ForeignKey[] = pragmaFks.map((r) => ({
    column: r.from,
    referencedTable: r.table,
    referencedColumn: r.to,
  }));

  // Indexes
  const pragmaIdxs = await query<{ name: string; unique: number; origin: string; partial: number }>(`PRAGMA index_list("${tableName}")`);
  const indexes: IndexInfo[] = [];
  for (const idx of pragmaIdxs) {
    const idxInfo = await query<{ name: string }>(`PRAGMA index_info("${idx.name}")`);
    let partialExpr: string | null = null;
    if (idx.partial) {
      const sqlRow = await query<{ sql: string }>(`SELECT sql FROM sqlite_master WHERE name = ?`, [idx.name]);
      const match = sqlRow[0]?.sql?.match(/WHERE\s+(.+)$/i);
      partialExpr = match?.[1] ?? null;
    }
    indexes.push({
      name: idx.name,
      columns: idxInfo.map((c) => c.name),
      unique: idx.unique === 1,
      type: null,
      isPrimary: idx.origin === 'pk',
      cardinality: null,
      partial: partialExpr,
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

  const lines: string[] = [`Table: ${desc.name} (${dialectName(dialect)})`, ''];

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
      const flags: string[] = [];
      if (idx.isPrimary) flags.push('primary');
      if (idx.unique && !idx.isPrimary) flags.push('unique');
      if (idx.cardinality != null && idx.cardinality > 0) flags.push(`~${idx.cardinality.toLocaleString()} distinct`);
      if (idx.partial) flags.push(`partial: ${idx.partial}`);
      const typeStr = idx.type ?? '';
      const parts = [typeStr, ...flags].filter(Boolean).join(', ');
      lines.push(`  ${idx.name}: (${idx.columns.join(', ')}) ${parts}`);
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
