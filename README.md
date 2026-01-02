# MSSQL to PostgreSQL Migration Script

A professional Node.js migration script that transfers all tables, columns, constraints, primary keys, foreign keys, and data from Microsoft SQL Server to PostgreSQL.

## Features

- **Complete Schema Migration**: Migrates tables with all columns, data types, and sizes
- **Constraint Handling**: Primary keys, unique constraints, and check constraints
- **Foreign Key Support**: Properly creates foreign keys after all tables are created
- **Data Type Mapping**: Comprehensive MSSQL to PostgreSQL type conversion
- **Identity/Serial Columns**: Handles auto-increment columns properly
- **Batch Processing**: Efficient data migration with configurable batch sizes
- **Professional Logging**: Detailed logs with timestamps, unique run IDs, and clear messages
- **Dependency Ordering**: Tables are migrated in correct order based on foreign key dependencies

## Prerequisites

- Node.js >= 16.0.0
- Access to source MSSQL database
- Access to target PostgreSQL server
- Sufficient permissions to create databases and tables

## Installation

```bash
cd /Users/apple/Desktop/APPS/migration
npm install
```

## Configuration

### Environment Variables

Create a `.env` file in the project root (copy from `.env.example`):

```bash
cp .env.example .env
```

Then edit the `.env` file with your database credentials:

#### Microsoft SQL Server Connection

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `MSSQL_HOST` | SQL Server hostname or IP address | `localhost` | Yes |
| `MSSQL_PORT` | SQL Server port | `1433` | No |
| `MSSQL_USER` | SQL Server username | - | Yes |
| `MSSQL_PASSWORD` | SQL Server password | - | Yes |
| `MSSQL_DATABASE` | Source database name | - | Yes |

#### PostgreSQL Connection

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PG_HOST` | PostgreSQL hostname or IP address | `localhost` | Yes |
| `PG_PORT` | PostgreSQL port | `5432` | No |
| `PG_USER` | PostgreSQL username | - | Yes |
| `PG_PASSWORD` | PostgreSQL password | - | Yes |
| `PG_DATABASE` | Target database name (will be created if not exists) | `ASDB` | No |

#### Migration Settings

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `BATCH_SIZE` | Number of rows to migrate per batch | `1000` | No |

### Example `.env` File

```env
# Microsoft SQL Server Connection
MSSQL_HOST=192.168.1.100
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=YourStrongPassword123
MSSQL_DATABASE=SourceDB

# PostgreSQL Connection
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=PostgresPassword123
PG_DATABASE=ASDB

# Migration Settings
BATCH_SIZE=1000
```

## Usage

Run the migration:

```bash
npm run migrate
```

Or directly:

```bash
node migrate.js
```

## Migration Process

The migration runs in three phases:

### Phase 1: Table Creation
- Creates all tables with columns and proper data types
- Applies primary key constraints
- Applies unique constraints
- Handles identity/serial columns

### Phase 2: Foreign Key Creation
- Creates all foreign key constraints after tables exist
- Preserves ON DELETE and ON UPDATE actions

### Phase 3: Data Migration
- Migrates data in batches (configurable via `BATCH_SIZE`)
- Tables are processed in dependency order
- Sequences are reset after data migration

## Logging

All logs are stored in the `logs/` directory with the following naming convention:

```
migration_YYYYMMDD_HHMMSS_LOGID.txt
```

Example: `migration_20260102_022700_A1B2C3D4.txt`

### Log Contents

- **Header**: Log ID, start time, file name
- **Connection Status**: Database connection details
- **Table Discovery**: List of tables found
- **Column Definitions**: Each column with data type and constraints
- **Primary Keys**: Primary key constraints created
- **Foreign Keys**: Foreign key relationships established
- **Data Migration Progress**: Batch-by-batch progress updates
- **Summary**: Final statistics including rows migrated, errors, and warnings

## Data Type Mappings

| MSSQL Type | PostgreSQL Type |
|------------|-----------------|
| `bigint` | `BIGINT` |
| `int` | `INTEGER` |
| `smallint` | `SMALLINT` |
| `tinyint` | `SMALLINT` |
| `bit` | `BOOLEAN` |
| `decimal(p,s)` | `NUMERIC(p,s)` |
| `numeric(p,s)` | `NUMERIC(p,s)` |
| `money` | `DECIMAL(19,4)` |
| `float` | `DOUBLE PRECISION` |
| `real` | `REAL` |
| `date` | `DATE` |
| `datetime` | `TIMESTAMP` |
| `datetime2` | `TIMESTAMP` |
| `datetimeoffset` | `TIMESTAMP WITH TIME ZONE` |
| `time` | `TIME` |
| `char(n)` | `CHAR(n)` |
| `varchar(n)` | `VARCHAR(n)` |
| `varchar(max)` | `TEXT` |
| `nchar(n)` | `CHAR(n)` |
| `nvarchar(n)` | `VARCHAR(n)` |
| `nvarchar(max)` | `TEXT` |
| `text` | `TEXT` |
| `ntext` | `TEXT` |
| `binary` | `BYTEA` |
| `varbinary` | `BYTEA` |
| `image` | `BYTEA` |
| `uniqueidentifier` | `UUID` |
| `xml` | `XML` |

## Troubleshooting

### Connection Issues

**MSSQL Connection Failed**
- Verify SQL Server is running and accepting TCP/IP connections
- Check firewall settings
- Ensure the user has appropriate permissions

**PostgreSQL Connection Failed**
- Verify PostgreSQL is running
- Check pg_hba.conf for connection permissions
- Ensure the user can create databases

### Migration Errors

**Foreign Key Creation Failed**
- Check that referenced tables exist
- Verify column data types match between tables

**Data Migration Failed**
- Check for data type incompatibilities
- Review the log file for specific error messages

## License

MIT
