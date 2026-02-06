import { getDialect, query } from './database.js';

interface ExplainResult {
  summary: string;
  indexesUsed: string[];
  sequentialScans: string[];
  estimatedRows: number | null;
  warnings: string[];
}

function formatRowCount(n: number): string {
  if (n >= 1_000_000) return `~${Math.round(n / 1_000_000)}M`;
  if (n >= 1_000) return `~${Math.round(n / 1_000)}K`;
  return `~${n}`;
}

// PostgreSQL: EXPLAIN (FORMAT JSON)

interface PgPlanNode {
  'Node Type': string;
  'Relation Name'?: string;
  'Index Name'?: string;
  'Plan Rows'?: number;
  Plans?: PgPlanNode[];
}

function walkPgPlan(node: PgPlanNode, result: ExplainResult): void {
  if (node['Node Type'] === 'Seq Scan' && node['Relation Name']) {
    result.sequentialScans.push(node['Relation Name']);
    const rows = node['Plan Rows'] ?? 0;
    result.warnings.push(
      `Sequential scan on '${node['Relation Name']}'${rows > 0 ? ` (${formatRowCount(rows)} rows)` : ''} — consider filtering on an indexed column`
    );
  }
  if (node['Index Name'] && !result.indexesUsed.includes(node['Index Name'])) {
    result.indexesUsed.push(node['Index Name']);
  }
  if (node['Plan Rows'] != null) {
    // Use the top-level estimated rows
    if (result.estimatedRows === null || node['Plan Rows'] > result.estimatedRows) {
      result.estimatedRows = node['Plan Rows'];
    }
  }
  if (node.Plans) {
    for (const child of node.Plans) {
      walkPgPlan(child, result);
    }
  }
}

interface PgExplainEntry {
  Plan: PgPlanNode;
}

async function explainPg(sql: string): Promise<ExplainResult> {
  const rows = await query<{ 'QUERY PLAN': PgExplainEntry[] }>(`EXPLAIN (FORMAT JSON) ${sql}`);
  const result: ExplainResult = {
    summary: '',
    indexesUsed: [],
    sequentialScans: [],
    estimatedRows: null,
    warnings: [],
  };

  const plan = rows[0]?.['QUERY PLAN']?.[0]?.Plan;
  if (plan) {
    result.estimatedRows = plan['Plan Rows'] ?? null;
    walkPgPlan(plan, result);
  }

  return result;
}

// MySQL: EXPLAIN FORMAT=JSON

interface MysqlTable {
  table_name?: string;
  access_type?: string;
  key?: string;
  rows_examined_per_scan?: number;
  attached_subqueries?: MysqlQueryBlock[];
}

interface MysqlQueryBlock {
  query_block?: {
    table?: MysqlTable;
    ordering_operation?: { table?: MysqlTable; nested_loop?: { table: MysqlTable }[] };
    nested_loop?: { table: MysqlTable }[];
    select_id?: number;
  };
}

function walkMysqlTables(obj: unknown, result: ExplainResult): void {
  if (obj == null || typeof obj !== 'object') return;

  const record = obj as Record<string, unknown>;

  // Check if this is a table object with access_type
  if ('access_type' in record && 'table_name' in record) {
    const table = record as unknown as MysqlTable;
    if (table.access_type === 'ALL' && table.table_name) {
      result.sequentialScans.push(table.table_name);
      const rows = table.rows_examined_per_scan ?? 0;
      result.warnings.push(
        `Sequential scan on '${table.table_name}'${rows > 0 ? ` (${formatRowCount(rows)} rows)` : ''} — consider filtering on an indexed column`
      );
    }
    if (table.key && !result.indexesUsed.includes(table.key)) {
      result.indexesUsed.push(table.key);
    }
    if (table.rows_examined_per_scan != null) {
      if (result.estimatedRows === null || table.rows_examined_per_scan > result.estimatedRows) {
        result.estimatedRows = table.rows_examined_per_scan;
      }
    }
  }

  // Recurse into all values
  for (const value of Object.values(record)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        walkMysqlTables(item, result);
      }
    } else if (typeof value === 'object' && value !== null) {
      walkMysqlTables(value, result);
    }
  }
}

async function explainMysql(sql: string): Promise<ExplainResult> {
  const rows = await query<{ EXPLAIN: string }>(`EXPLAIN FORMAT=JSON ${sql}`);
  const result: ExplainResult = {
    summary: '',
    indexesUsed: [],
    sequentialScans: [],
    estimatedRows: null,
    warnings: [],
  };

  const raw = rows[0]?.EXPLAIN;
  if (raw) {
    const parsed = JSON.parse(typeof raw === 'string' ? raw : JSON.stringify(raw));
    walkMysqlTables(parsed, result);
  }

  return result;
}

// SQLite: EXPLAIN QUERY PLAN

async function explainSqlite(sql: string): Promise<ExplainResult> {
  const rows = await query<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`);
  const result: ExplainResult = {
    summary: '',
    indexesUsed: [],
    sequentialScans: [],
    estimatedRows: null,
    warnings: [],
  };

  for (const row of rows) {
    const detail = row.detail;

    // "SCAN table" or "SCAN table USING ..." = sequential scan
    const scanMatch = detail.match(/^SCAN\s+(\S+)/);
    if (scanMatch) {
      // Check if it's using an index (covering index scan)
      const usingIndex = detail.match(/USING\s+(?:COVERING\s+)?INDEX\s+(\S+)/);
      if (usingIndex) {
        if (!result.indexesUsed.includes(usingIndex[1])) {
          result.indexesUsed.push(usingIndex[1]);
        }
      } else {
        result.sequentialScans.push(scanMatch[1]);
        result.warnings.push(
          `Sequential scan on '${scanMatch[1]}' — consider filtering on an indexed column`
        );
      }
    }

    // "SEARCH table USING INDEX idx (...)" = index usage
    const searchMatch = detail.match(/SEARCH\s+\S+\s+USING\s+(?:COVERING\s+)?INDEX\s+(\S+)/);
    if (searchMatch && !result.indexesUsed.includes(searchMatch[1])) {
      result.indexesUsed.push(searchMatch[1]);
    }
  }

  return result;
}

// Dispatcher

export async function explainQuery(sql: string): Promise<ExplainResult> {
  const dialect = getDialect();

  let result: ExplainResult;
  if (dialect === 'pg') result = await explainPg(sql);
  else if (dialect === 'mysql2') result = await explainMysql(sql);
  else result = await explainSqlite(sql);

  // Add general warnings
  if (result.indexesUsed.length === 0 && result.sequentialScans.length > 0) {
    result.warnings.push('No indexes used — check describe_table for available indexes');
  }

  // Build summary
  const parts: string[] = [];
  if (result.estimatedRows != null) {
    parts.push(`${formatRowCount(result.estimatedRows)} rows estimated`);
  }
  if (result.indexesUsed.length > 0) {
    parts.push(`uses ${result.indexesUsed.length} index(es)`);
  }
  if (result.sequentialScans.length > 0) {
    parts.push(`${result.sequentialScans.length} sequential scan(s)`);
  }
  result.summary = parts.length > 0 ? parts.join(', ') : 'No plan details available';

  return result;
}

export function formatExplainResult(result: ExplainResult): string {
  const lines: string[] = ['Query Plan Analysis'];

  if (result.estimatedRows != null) {
    lines.push(`  Estimated rows: ${formatRowCount(result.estimatedRows)}`);
  }

  lines.push(
    `  Indexes used: ${result.indexesUsed.length > 0 ? result.indexesUsed.join(', ') : '(none)'}`
  );

  lines.push(
    `  Sequential scans: ${result.sequentialScans.length > 0 ? result.sequentialScans.join(', ') : 'none'}`
  );

  if (result.warnings.length > 0) {
    lines.push('');
    lines.push('  Warnings:');
    for (const w of result.warnings) {
      lines.push(`    - ${w}`);
    }
  }

  return lines.join('\n');
}
