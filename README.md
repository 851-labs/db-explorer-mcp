# DB Explorer MCP

Connect to any SQL database and explore schemas, run queries, and visualize results with interactive charts — all from Claude Desktop.

## Install

```
npx @anthropic-ai/mcpb add db-explorer-mcp
```

Or add manually to your Claude Desktop config:

```json
{
  "mcpServers": {
    "db-explorer": {
      "command": "npx",
      "args": ["db-explorer-mcp"],
      "env": {
        "DB_CONNECTION_STRING": "postgres://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

## Supported databases

- **PostgreSQL** — `postgres://user:pass@host:5432/db`
- **MySQL** — `mysql://user:pass@host:3306/db`
- **SQLite** — `sqlite:///path/to/file.db`

## Tools

| Tool | Description |
|------|-------------|
| `connect` | Connect to a database via connection string |
| `list_tables` | List all tables with estimated row counts |
| `describe_table` | Describe columns, types, constraints, and foreign keys |
| `get_schema` | Get the full database schema with relationships |
| `query` | Execute a read-only SQL query |
| `chart` | Render an interactive chart from query results |

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

## Support

If you like this project, please consider giving it a star.
