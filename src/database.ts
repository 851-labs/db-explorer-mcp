import Knex, { type Knex as KnexType } from 'knex';

let knexInstance: KnexType | null = null;
let currentDialect: 'pg' | 'mysql2' | 'better-sqlite3' | null = null;

export type Dialect = 'pg' | 'mysql2' | 'better-sqlite3';

function isHostedProvider(hostname: string): boolean {
  const providers = ['psdb.cloud', 'aivencloud.com', 'amazonaws.com', 'azure.com', 'cloud.google.com', 'neon.tech', 'supabase.co', 'planetscale.com'];
  return providers.some(p => hostname.endsWith(p));
}

export function parseConnectionString(str: string): { client: Dialect; connection: any } {
  const trimmed = str.trim();

  // PostgreSQL
  if (trimmed.startsWith('postgres://') || trimmed.startsWith('postgresql://')) {
    const url = new URL(trimmed);
    const ssl = url.searchParams.get('ssl') || url.searchParams.get('sslmode');
    if (ssl && ssl !== 'disable' && ssl !== 'false') {
      url.searchParams.delete('ssl');
      url.searchParams.delete('sslmode');
      return { client: 'pg', connection: { connectionString: url.toString(), ssl: { rejectUnauthorized: ssl !== 'no-verify' } } };
    }
    return { client: 'pg', connection: trimmed };
  }

  // MySQL
  if (trimmed.startsWith('mysql://')) {
    const url = new URL(trimmed);
    const ssl = url.searchParams.get('ssl');
    // Auto-enable SSL for known hosted providers
    const needsSsl = ssl || isHostedProvider(url.hostname);
    if (needsSsl) {
      url.searchParams.delete('ssl');
      return {
        client: 'mysql2',
        connection: {
          host: url.hostname,
          port: parseInt(url.port || '3306'),
          user: url.username,
          password: decodeURIComponent(url.password),
          database: url.pathname.slice(1),
          ssl: { rejectUnauthorized: true },
        },
      };
    }
    return { client: 'mysql2', connection: trimmed };
  }

  // SQLite: "sqlite:///path/to/file" or "sqlite:/path/to/file" or just a file path
  if (trimmed.startsWith('sqlite:')) {
    const filePath = trimmed.replace(/^sqlite:\/\//, '').replace(/^sqlite:/, '');
    return { client: 'better-sqlite3', connection: { filename: filePath || ':memory:' } };
  }

  // Bare file path → SQLite
  if (trimmed.endsWith('.db') || trimmed.endsWith('.sqlite') || trimmed.endsWith('.sqlite3') || trimmed === ':memory:') {
    return { client: 'better-sqlite3', connection: { filename: trimmed } };
  }

  throw new Error(
    `Unrecognized connection string format: "${trimmed}"\n` +
    'Expected: postgres://..., mysql://..., sqlite:///path, or a .db/.sqlite file path'
  );
}

export async function connect(connectionString: string): Promise<string> {
  // Destroy existing connection if any
  if (knexInstance) {
    await knexInstance.destroy();
    knexInstance = null;
    currentDialect = null;
  }

  const { client, connection } = parseConnectionString(connectionString);

  knexInstance = Knex({
    client,
    connection,
    pool: client === 'better-sqlite3' ? { min: 1, max: 1 } : { min: 0, max: 5 },
    useNullAsDefault: client === 'better-sqlite3',
  });

  // Test the connection
  await knexInstance.raw('SELECT 1');

  // Enforce read-only mode
  if (client === 'pg') {
    await knexInstance.raw('SET default_transaction_read_only = ON');
  } else if (client === 'mysql2') {
    await knexInstance.raw('SET SESSION TRANSACTION READ ONLY');
  } else if (client === 'better-sqlite3') {
    await knexInstance.raw('PRAGMA query_only = ON');
  }

  currentDialect = client;

  const dialectName = client === 'pg' ? 'PostgreSQL' : client === 'mysql2' ? 'MySQL' : 'SQLite';
  return `Connected to ${dialectName} database (read-only mode)`;
}

export function getKnex(): KnexType {
  if (!knexInstance) {
    throw new Error(
      'Not connected to any database. Use the connect tool with a connection string, ' +
      'or set the DB_CONNECTION_STRING environment variable.'
    );
  }
  return knexInstance;
}

export function getDialect(): Dialect {
  if (!currentDialect) {
    throw new Error('Not connected to any database.');
  }
  return currentDialect;
}

export async function query<T extends Record<string, unknown> = Record<string, unknown>>(
  sql: string,
  params?: unknown[]
): Promise<T[]> {
  const knex = getKnex();
  const result = params ? await knex.raw(sql, params) : await knex.raw(sql);
  // MySQL returns [rows, fields]
  if (Array.isArray(result) && result.length === 2 && Array.isArray(result[0]) && Array.isArray(result[1])) {
    return result[0] as T[];
  }
  // PostgreSQL returns { rows: [...] }
  if (result.rows && Array.isArray(result.rows)) {
    return result.rows as T[];
  }
  // SQLite returns array directly
  if (Array.isArray(result)) {
    return result as T[];
  }
  return result as T[];
}

export async function disconnect(): Promise<void> {
  if (knexInstance) {
    await knexInstance.destroy();
    knexInstance = null;
    currentDialect = null;
  }
}

export function redactConnectionString(str: string): string {
  // Redact password in connection strings like postgres://user:password@host/db
  return str.replace(/:\/\/([^:]+):([^@]+)@/, '://$1:***@');
}
