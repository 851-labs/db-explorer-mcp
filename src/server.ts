#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  registerAppTool,
  registerAppResource,
  RESOURCE_MIME_TYPE,
} from '@modelcontextprotocol/ext-apps/server';
import { z } from 'zod';
import * as fs from 'fs';
import * as path from 'path';
import {
  connect as dbConnect,
  getDialect,
  getKnex,
  query,
  redactConnectionString,
} from './database.js';
import {
  listTables,
  describeTable,
  getFullSchema,
} from './introspection.js';
import { explainQuery, formatExplainResult } from './explain.js';

console.error('DB Explorer MCP Server starting...');

// Auto-connect from env var (lazy — triggered on first tool call that needs a connection)
let autoConnectAttempted = false;

async function ensureConnection(): Promise<void> {
  try {
    getKnex(); // Will throw if not connected
  } catch {
    if (!autoConnectAttempted && process.env.DB_CONNECTION_STRING) {
      autoConnectAttempted = true;
      const cs = process.env.DB_CONNECTION_STRING;
      console.error(`  Auto-connecting from DB_CONNECTION_STRING: ${redactConnectionString(cs)}`);
      await dbConnect(cs);
    } else {
      throw new Error(
        'Not connected to any database. Use the connect tool with a connection string, ' +
        'or set the DB_CONNECTION_STRING environment variable.'
      );
    }
  }
}

// Create MCP server
const server = new McpServer({
  name: 'db-explorer',
  version: '1.0.0',
});

// MCP App resource URI
const CHART_RESOURCE_URI = 'ui://db-explorer/chart-app.html';

// Register the chart UI resource
const chartHtmlPath = path.join(import.meta.dirname, 'chart-app.html');
if (!fs.existsSync(chartHtmlPath)) {
  console.error(`Warning: chart-app.html not found at ${chartHtmlPath}. Chart tool will be unavailable.`);
}

registerAppResource(
  server,
  'DB Explorer Chart',
  CHART_RESOURCE_URI,
  {
    description: 'Interactive chart UI for database query results',
  },
  async () => {
    const html = fs.readFileSync(chartHtmlPath, 'utf-8');
    return {
      contents: [
        { uri: CHART_RESOURCE_URI, mimeType: RESOURCE_MIME_TYPE, text: html },
      ],
    };
  }
);

// ===== TOOLS =====

// Tool: Connect to database
server.tool(
  'connect',
  'Connect to a SQL database (PostgreSQL, MySQL, or SQLite) via connection string. ' +
  'Examples: postgres://user:pass@host/db, mysql://user:pass@host/db, sqlite:///path/to/file.db',
  {
    connectionString: z.string().describe(
      'Database connection string (postgres://, mysql://, sqlite:, or a .db file path)'
    ),
  },
  async ({ connectionString }) => {
    try {
      const message = await dbConnect(connectionString);
      return {
        content: [{
          type: 'text',
          text: `${message}\nConnection: ${redactConnectionString(connectionString)}`,
        }],
      };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Connection failed';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }
  }
);

// Tool: List tables
server.tool(
  'list_tables',
  'List all tables in the connected database with estimated row counts',
  {},
  async () => {
    try {
      await ensureConnection();
      const result = await listTables();
      return { content: [{ type: 'text', text: result }] };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Failed to list tables';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }
  }
);

// Tool: Describe table
server.tool(
  'describe_table',
  'Get columns, types, constraints, foreign keys, and indexes for a specific table. ' +
  'Review indexes before writing queries to identify which columns can be filtered ' +
  'and sorted efficiently, and use foreign keys to determine JOIN conditions.',
  {
    table: z.string().describe('Table name to describe'),
  },
  async ({ table }) => {
    try {
      await ensureConnection();
      const result = await describeTable(table);
      return { content: [{ type: 'text', text: result }] };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Failed to describe table';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }
  }
);

// Tool: Get full schema
server.tool(
  'get_schema',
  'Get the full database schema including all tables, columns, types, relationships, and indexes',
  {},
  async () => {
    try {
      await ensureConnection();
      const result = await getFullSchema();
      return { content: [{ type: 'text', text: result }] };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Failed to get schema';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }
  }
);

// Dialect-aware error hint for failed queries
function dialectHint(): string {
  try {
    const d = getDialect();
    if (d === 'mysql2') return ' (MySQL — use DATE_SUB(), CURDATE(), IFNULL, backtick-quoted identifiers)';
    if (d === 'pg') return ' (PostgreSQL — use CURRENT_DATE - INTERVAL, COALESCE, double-quoted identifiers)';
    return ' (SQLite — use date(), julianday(), IFNULL, no GROUP BY alias references)';
  } catch {
    return '';
  }
}

// Tool: Execute query
const MAX_QUERY_ROWS = 1000;

server.tool(
  'query',
  `Execute a read-only SQL query. Returns JSON array of results (max ${MAX_QUERY_ROWS} rows). ` +
  'Use JOINs to fetch related data in a single query instead of making multiple ' +
  'separate queries. Filter and sort on indexed columns when possible — use ' +
  'describe_table to check available indexes. Always include a LIMIT clause unless you ' +
  'need all rows. For complex queries, use explain_query first to verify the plan uses indexes.',
  {
    sql: z.string().describe('SELECT or WITH query to execute'),
  },
  async ({ sql }) => {
    try {
      await ensureConnection();
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Not connected';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }

    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith('SELECT') && !trimmed.startsWith('WITH')) {
      return {
        content: [{ type: 'text', text: 'Error: Only SELECT and WITH (CTE) queries are allowed' }],
        isError: true,
      };
    }

    try {
      const rows = await query(sql);

      const truncated = rows.length > MAX_QUERY_ROWS;
      const data = truncated ? rows.slice(0, MAX_QUERY_ROWS) : rows;
      const text = JSON.stringify(data, null, 2);

      return {
        content: [{
          type: 'text',
          text: truncated
            ? `${text}\n\n(Showing ${MAX_QUERY_ROWS} of ${rows.length} rows. Add a LIMIT clause for smaller result sets.)`
            : text,
        }],
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Query execution failed';
      return { content: [{ type: 'text', text: `Error: ${message}${dialectHint()}` }], isError: true };
    }
  }
);

// Tool: Explain query
server.tool(
  'explain_query',
  'Show the execution plan for a SELECT query without running it. ' +
  'Returns which indexes are used, estimated rows, and warnings about sequential scans. ' +
  'Use this before running expensive queries to verify they use indexes efficiently.',
  {
    sql: z.string().describe('SELECT or WITH query to analyze'),
  },
  async ({ sql }) => {
    try {
      await ensureConnection();
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Not connected';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }

    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith('SELECT') && !trimmed.startsWith('WITH')) {
      return {
        content: [{ type: 'text', text: 'Error: Only SELECT and WITH (CTE) queries are allowed' }],
        isError: true,
      };
    }

    try {
      const result = await explainQuery(sql);
      return { content: [{ type: 'text', text: formatExplainResult(result) }] };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Explain failed';
      return { content: [{ type: 'text', text: `Error: ${message}` }], isError: true };
    }
  }
);

// ===== CHART TOOL (MCP Apps) =====

registerAppTool(
  server,
  'chart',
  {
    title: 'Chart',
    description:
      'Render an interactive chart from a SQL query. ' +
      'The query columns determine the chart axes. ' +
      'First column becomes the x-axis, remaining columns become data series.',
    inputSchema: {
      sql: z.string().describe('SELECT query to execute'),
      title: z.string().describe('Chart title'),
      chartType: z.enum(['area', 'bar', 'line', 'pie']).describe('Chart type'),
      xAxis: z.string().optional().describe('Column name for x-axis (default: first column)'),
      series: z.array(z.string()).optional().describe('Column names to plot as data series (default: all non-xAxis columns)'),
      stacked: z.boolean().optional().describe('Stack multi-series data (default: false)'),
      description: z.string().optional().describe('Optional subtitle'),
    },
    _meta: { ui: { resourceUri: CHART_RESOURCE_URI } },
  },
  async ({ sql, title, chartType, xAxis, series, stacked, description }) => {
    try {
      await ensureConnection();
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Not connected';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }

    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith('SELECT') && !trimmed.startsWith('WITH')) {
      return {
        content: [{ type: 'text', text: 'Error: Only SELECT and WITH (CTE) queries are allowed' }],
        isError: true,
      };
    }

    try {
      const data = await query(sql);

      if (data.length === 0) {
        return { content: [{ type: 'text', text: 'Query returned no results' }], isError: true };
      }

      const columns = Object.keys(data[0]);
      const xAxisKey = xAxis || columns[0];
      const seriesColumns = series || columns.filter(c => c !== xAxisKey);

      const config: Record<string, unknown> = {
        title,
        description,
        chartType,
        data,
        xAxisKey,
        dataKey: seriesColumns[0],
      };

      if (seriesColumns.length > 1) {
        config.multiSeries = seriesColumns;
      }

      if (stacked) {
        config.stacked = true;
      }

      return {
        content: [{ type: 'text', text: JSON.stringify(config) }],
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Chart query failed';
      return { content: [{ type: 'text', text: `Error: ${message}${dialectHint()}` }], isError: true };
    }
  }
);

// Main
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('DB Explorer MCP Server running on stdio');
}

main().catch((err) => {
  console.error('Fatal:', err instanceof Error ? err.message : err);
  process.exit(1);
});
