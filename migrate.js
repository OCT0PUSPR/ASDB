/**
 * MSSQL to PostgreSQL Migration Script
 * 
 * This script migrates all tables, columns, constraints, primary keys,
 * foreign keys, and data from Microsoft SQL Server to PostgreSQL.
 */

require('dotenv').config();
const sql = require('mssql');
const { Pool, Client } = require('pg');
const MigrationLogger = require('./logger');
const { mapDataType, escapeIdentifier, convertDefaultValue } = require('./type-mapper');

// Configuration
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

// MSSQL Configuration
const mssqlConfig = {
  server: process.env.MSSQL_HOST || 'localhost',
  port: parseInt(process.env.MSSQL_PORT) || 1433,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASSWORD,
  database: process.env.MSSQL_DATABASE,
  options: {
    encrypt: false,
    trustServerCertificate: true,
    enableArithAbort: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// PostgreSQL Configuration
const pgConfig = {
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT) || 5432,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE || 'ASDB',
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
};

// Statistics tracking
const stats = {
  tablesProcessed: 0,
  tablesCreated: 0,
  primaryKeysCreated: 0,
  foreignKeysCreated: 0,
  totalRowsMigrated: 0,
  errors: 0,
  warnings: 0
};

let logger;
let mssqlPool;
let pgPool;

/**
 * Initialize database connections
 */
async function initializeConnections() {
  logger.section('DATABASE CONNECTION INITIALIZATION');
  
  // Connect to MSSQL
  logger.info('CONNECTION', `Connecting to MSSQL Server at ${mssqlConfig.server}:${mssqlConfig.port}`);
  try {
    mssqlPool = await sql.connect(mssqlConfig);
    logger.success('CONNECTION', `Successfully connected to MSSQL database "${mssqlConfig.database}"`);
  } catch (error) {
    logger.error('CONNECTION', `Failed to connect to MSSQL: ${error.message}`);
    throw error;
  }

  // Connect to PostgreSQL (first to default database to create target DB if needed)
  logger.info('CONNECTION', `Connecting to PostgreSQL Server at ${pgConfig.host}:${pgConfig.port}`);
  
  try {
    // First, try to create the database if it doesn't exist
    const adminClient = new Client({
      host: pgConfig.host,
      port: pgConfig.port,
      user: pgConfig.user,
      password: pgConfig.password,
      database: 'postgres'
    });
    
    await adminClient.connect();
    logger.success('CONNECTION', 'Connected to PostgreSQL admin database');
    
    // Check if database exists
    const dbCheckResult = await adminClient.query(
      `SELECT 1 FROM pg_database WHERE datname = $1`,
      [pgConfig.database]
    );
    
    if (dbCheckResult.rows.length === 0) {
      logger.info('DATABASE', `Creating database "${pgConfig.database}"...`);
      await adminClient.query(`CREATE DATABASE "${pgConfig.database}"`);
      logger.success('DATABASE', `Database "${pgConfig.database}" created successfully`);
    } else {
      logger.info('DATABASE', `Database "${pgConfig.database}" already exists`);
    }
    
    await adminClient.end();
    
    // Now connect to the target database
    pgPool = new Pool(pgConfig);
    await pgPool.query('SELECT 1');
    logger.success('CONNECTION', `Successfully connected to PostgreSQL database "${pgConfig.database}"`);
    
  } catch (error) {
    logger.error('CONNECTION', `Failed to connect to PostgreSQL: ${error.message}`);
    throw error;
  }
}

/**
 * Get all user tables from MSSQL
 */
async function getTables() {
  logger.section('TABLE DISCOVERY');
  logger.info('DISCOVERY', 'Retrieving list of tables from MSSQL database...');
  
  const query = `
    SELECT 
      t.TABLE_SCHEMA as schema_name,
      t.TABLE_NAME as table_name
    FROM INFORMATION_SCHEMA.TABLES t
    WHERE t.TABLE_TYPE = 'BASE TABLE'
      AND t.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
  `;
  
  const result = await mssqlPool.request().query(query);
  const tables = result.recordset;
  
  logger.success('DISCOVERY', `Found ${tables.length} tables to migrate`, {
    'Tables': tables.map(t => `${t.schema_name}.${t.table_name}`).join(', ')
  });
  
  return tables;
}

/**
 * Get column information for a table
 */
async function getColumns(schemaName, tableName) {
  const query = `
    SELECT 
      c.COLUMN_NAME as name,
      c.DATA_TYPE as dataType,
      c.CHARACTER_MAXIMUM_LENGTH as maxLength,
      c.NUMERIC_PRECISION as precision,
      c.NUMERIC_SCALE as scale,
      c.IS_NULLABLE as isNullable,
      c.COLUMN_DEFAULT as defaultValue,
      c.ORDINAL_POSITION as ordinalPosition,
      COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as isIdentity
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = @schema
      AND c.TABLE_NAME = @table
    ORDER BY c.ORDINAL_POSITION
  `;
  
  const result = await mssqlPool.request()
    .input('schema', sql.NVarChar, schemaName)
    .input('table', sql.NVarChar, tableName)
    .query(query);
  
  return result.recordset.map(col => ({
    name: col.name,
    dataType: col.dataType,
    maxLength: col.maxLength,
    precision: col.precision,
    scale: col.scale,
    isNullable: col.isNullable === 'YES',
    defaultValue: col.defaultValue,
    ordinalPosition: col.ordinalPosition,
    isIdentity: col.isIdentity === 1
  }));
}

/**
 * Get primary key information for a table
 */
async function getPrimaryKey(schemaName, tableName) {
  const query = `
    SELECT 
      kc.CONSTRAINT_NAME as constraintName,
      kc.COLUMN_NAME as columnName,
      kc.ORDINAL_POSITION as ordinalPosition
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      ON kc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
      AND kc.TABLE_SCHEMA = tc.TABLE_SCHEMA
      AND kc.TABLE_NAME = tc.TABLE_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND kc.TABLE_SCHEMA = @schema
      AND kc.TABLE_NAME = @table
    ORDER BY kc.ORDINAL_POSITION
  `;
  
  const result = await mssqlPool.request()
    .input('schema', sql.NVarChar, schemaName)
    .input('table', sql.NVarChar, tableName)
    .query(query);
  
  if (result.recordset.length === 0) {
    return null;
  }
  
  return {
    name: result.recordset[0].constraintName,
    columns: result.recordset.map(r => r.columnName)
  };
}

/**
 * Get unique constraints for a table
 */
async function getUniqueConstraints(schemaName, tableName) {
  const query = `
    SELECT 
      tc.CONSTRAINT_NAME as constraintName,
      kc.COLUMN_NAME as columnName,
      kc.ORDINAL_POSITION as ordinalPosition
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
      ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
      AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA
      AND tc.TABLE_NAME = kc.TABLE_NAME
    WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
      AND tc.TABLE_SCHEMA = @schema
      AND tc.TABLE_NAME = @table
    ORDER BY tc.CONSTRAINT_NAME, kc.ORDINAL_POSITION
  `;
  
  const result = await mssqlPool.request()
    .input('schema', sql.NVarChar, schemaName)
    .input('table', sql.NVarChar, tableName)
    .query(query);
  
  // Group by constraint name
  const constraints = {};
  for (const row of result.recordset) {
    if (!constraints[row.constraintName]) {
      constraints[row.constraintName] = {
        name: row.constraintName,
        columns: []
      };
    }
    constraints[row.constraintName].columns.push(row.columnName);
  }
  
  return Object.values(constraints);
}

/**
 * Get check constraints for a table
 */
async function getCheckConstraints(schemaName, tableName) {
  const query = `
    SELECT 
      cc.CONSTRAINT_NAME as constraintName,
      cc.CHECK_CLAUSE as checkClause
    FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
    INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
      ON cc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
      AND cc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
    WHERE ccu.TABLE_SCHEMA = @schema
      AND ccu.TABLE_NAME = @table
  `;
  
  const result = await mssqlPool.request()
    .input('schema', sql.NVarChar, schemaName)
    .input('table', sql.NVarChar, tableName)
    .query(query);
  
  return result.recordset.map(r => ({
    name: r.constraintName,
    clause: r.checkClause
  }));
}

/**
 * Get all foreign keys from MSSQL database
 */
async function getAllForeignKeys() {
  logger.section('FOREIGN KEY DISCOVERY');
  logger.info('DISCOVERY', 'Retrieving all foreign key relationships from MSSQL database...');
  
  const query = `
    SELECT 
      fk.name as constraintName,
      OBJECT_SCHEMA_NAME(fk.parent_object_id) as tableSchema,
      OBJECT_NAME(fk.parent_object_id) as tableName,
      COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as columnName,
      OBJECT_SCHEMA_NAME(fk.referenced_object_id) as referencedSchema,
      OBJECT_NAME(fk.referenced_object_id) as referencedTable,
      COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as referencedColumn,
      fk.delete_referential_action_desc as deleteAction,
      fk.update_referential_action_desc as updateAction,
      fkc.constraint_column_id as ordinalPosition
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc
      ON fk.object_id = fkc.constraint_object_id
    WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY fk.name, fkc.constraint_column_id
  `;
  
  const result = await mssqlPool.request().query(query);
  
  // Group by constraint name
  const foreignKeys = {};
  for (const row of result.recordset) {
    const key = `${row.tableSchema}.${row.tableName}.${row.constraintName}`;
    if (!foreignKeys[key]) {
      foreignKeys[key] = {
        name: row.constraintName,
        tableSchema: row.tableSchema,
        tableName: row.tableName,
        columns: [],
        referencedSchema: row.referencedSchema,
        referencedTable: row.referencedTable,
        referencedColumns: [],
        deleteAction: row.deleteAction,
        updateAction: row.updateAction
      };
    }
    foreignKeys[key].columns.push(row.columnName);
    foreignKeys[key].referencedColumns.push(row.referencedColumn);
  }
  
  const fkList = Object.values(foreignKeys);
  logger.success('DISCOVERY', `Found ${fkList.length} foreign key constraints`);
  
  return fkList;
}

/**
 * Convert MSSQL referential action to PostgreSQL
 */
function convertReferentialAction(action) {
  const mappings = {
    'NO_ACTION': 'NO ACTION',
    'CASCADE': 'CASCADE',
    'SET_NULL': 'SET NULL',
    'SET_DEFAULT': 'SET DEFAULT'
  };
  return mappings[action] || 'NO ACTION';
}

/**
 * Create a table in PostgreSQL
 */
async function createTable(schemaName, tableName, columns, primaryKey, uniqueConstraints) {
  const fullTableName = `${schemaName}_${tableName}`;
  const pgTableName = escapeIdentifier(fullTableName);
  
  logger.tableInfo(fullTableName, 'Starting table creation', {
    'Columns': columns.length,
    'Has Primary Key': primaryKey ? 'Yes' : 'No',
    'Unique Constraints': uniqueConstraints.length
  });
  
  // Build column definitions
  const columnDefs = [];
  for (const col of columns) {
    const pgType = mapDataType(col.dataType, col.maxLength, col.precision, col.scale);
    const constraints = [];
    
    // Handle identity columns
    let finalType = pgType;
    if (col.isIdentity) {
      if (pgType === 'BIGINT') {
        finalType = 'BIGSERIAL';
      } else if (pgType === 'SMALLINT') {
        finalType = 'SMALLSERIAL';
      } else {
        finalType = 'SERIAL';
      }
      constraints.push('IDENTITY/SERIAL');
    }
    
    // NOT NULL
    if (!col.isNullable && !col.isIdentity) {
      constraints.push('NOT NULL');
    }
    
    // Default value (skip for identity columns)
    let defaultClause = '';
    if (col.defaultValue && !col.isIdentity) {
      const pgDefault = convertDefaultValue(col.defaultValue, pgType);
      if (pgDefault) {
        defaultClause = ` DEFAULT ${pgDefault}`;
        constraints.push(`DEFAULT ${pgDefault}`);
      }
    }
    
    const nullClause = (!col.isNullable && !col.isIdentity) ? ' NOT NULL' : '';
    const colDef = `${escapeIdentifier(col.name)} ${finalType}${nullClause}${defaultClause}`;
    columnDefs.push(colDef);
    
    logger.columnInfo(fullTableName, col.name, finalType, constraints);
  }
  
  // Add primary key constraint
  if (primaryKey) {
    const pkColumns = primaryKey.columns.map(c => escapeIdentifier(c)).join(', ');
    const pkName = escapeIdentifier(`pk_${fullTableName}`);
    columnDefs.push(`CONSTRAINT ${pkName} PRIMARY KEY (${pkColumns})`);
    logger.primaryKeyInfo(fullTableName, `pk_${fullTableName}`, primaryKey.columns);
  }
  
  // Add unique constraints
  for (const uc of uniqueConstraints) {
    const ucColumns = uc.columns.map(c => escapeIdentifier(c)).join(', ');
    const ucName = escapeIdentifier(`uq_${fullTableName}_${uc.columns.join('_')}`);
    columnDefs.push(`CONSTRAINT ${ucName} UNIQUE (${ucColumns})`);
    logger.info('UNIQUE_CONSTRAINT', `Adding unique constraint on table "${fullTableName}"`, {
      'Columns': uc.columns.join(', ')
    });
  }
  
  // Build and execute CREATE TABLE statement
  const createTableSQL = `CREATE TABLE IF NOT EXISTS ${pgTableName} (\n  ${columnDefs.join(',\n  ')}\n)`;
  
  try {
    await pgPool.query(createTableSQL);
    logger.success('TABLE_CREATION', `Successfully created table "${fullTableName}"`, {
      'Columns': columns.length,
      'Primary Key': primaryKey ? primaryKey.columns.join(', ') : 'None'
    });
    stats.tablesCreated++;
    if (primaryKey) stats.primaryKeysCreated++;
    return true;
  } catch (error) {
    logger.error('TABLE_CREATION', `Failed to create table "${fullTableName}": ${error.message}`, {
      'SQL': createTableSQL.substring(0, 500)
    });
    stats.errors++;
    return false;
  }
}

/**
 * Create foreign keys in PostgreSQL
 */
async function createForeignKeys(foreignKeys) {
  logger.section('FOREIGN KEY CREATION');
  logger.info('FK_CREATION', `Creating ${foreignKeys.length} foreign key constraints...`);
  
  for (const fk of foreignKeys) {
    const sourceTable = `${fk.tableSchema}_${fk.tableName}`;
    const targetTable = `${fk.referencedSchema}_${fk.referencedTable}`;
    const fkName = escapeIdentifier(`fk_${sourceTable}_${fk.columns.join('_')}`);
    
    const sourceColumns = fk.columns.map(c => escapeIdentifier(c)).join(', ');
    const targetColumns = fk.referencedColumns.map(c => escapeIdentifier(c)).join(', ');
    
    const deleteAction = convertReferentialAction(fk.deleteAction);
    const updateAction = convertReferentialAction(fk.updateAction);
    
    const alterSQL = `
      ALTER TABLE ${escapeIdentifier(sourceTable)}
      ADD CONSTRAINT ${fkName}
      FOREIGN KEY (${sourceColumns})
      REFERENCES ${escapeIdentifier(targetTable)} (${targetColumns})
      ON DELETE ${deleteAction}
      ON UPDATE ${updateAction}
    `;
    
    logger.foreignKeyInfo(sourceTable, fkName.replace(/"/g, ''), targetTable, 
      `${fk.columns.join(', ')} -> ${fk.referencedColumns.join(', ')}`);
    
    try {
      await pgPool.query(alterSQL);
      logger.success('FK_CREATION', `Successfully created foreign key "${fkName.replace(/"/g, '')}" on table "${sourceTable}"`, {
        'References': `${targetTable}(${fk.referencedColumns.join(', ')})`,
        'On Delete': deleteAction,
        'On Update': updateAction
      });
      stats.foreignKeysCreated++;
    } catch (error) {
      logger.error('FK_CREATION', `Failed to create foreign key on table "${sourceTable}": ${error.message}`, {
        'Constraint': fkName,
        'Target': targetTable
      });
      stats.errors++;
    }
  }
}

/**
 * Get row count for a table
 */
async function getRowCount(schemaName, tableName) {
  const query = `SELECT COUNT(*) as count FROM [${schemaName}].[${tableName}]`;
  const result = await mssqlPool.request().query(query);
  return result.recordset[0].count;
}

/**
 * Migrate data for a single table
 */
async function migrateTableData(schemaName, tableName, columns) {
  const fullTableName = `${schemaName}_${tableName}`;
  const pgTableName = escapeIdentifier(fullTableName);
  
  logger.info('DATA_MIGRATION', `Starting data migration for table "${fullTableName}"...`);
  
  try {
    // Get total row count
    const totalRows = await getRowCount(schemaName, tableName);
    
    if (totalRows === 0) {
      logger.info('DATA_MIGRATION', `Table "${fullTableName}" is empty, skipping data migration`);
      return true;
    }
    
    logger.info('DATA_MIGRATION', `Table "${fullTableName}" has ${totalRows} rows to migrate`);
    
    // Build column list for SELECT and INSERT
    const columnNames = columns.map(c => c.name);
    const selectColumns = columnNames.map(c => `[${c}]`).join(', ');
    const insertColumns = columnNames.map(c => escapeIdentifier(c)).join(', ');
    
    // Migrate data in batches
    let offset = 0;
    let batchNumber = 0;
    let totalMigrated = 0;
    
    // Determine if table has a primary key or unique column for ordering
    const orderByColumn = columns[0].name;
    
    while (offset < totalRows) {
      batchNumber++;
      
      // Fetch batch from MSSQL
      const selectQuery = `
        SELECT ${selectColumns}
        FROM [${schemaName}].[${tableName}]
        ORDER BY [${orderByColumn}]
        OFFSET ${offset} ROWS
        FETCH NEXT ${BATCH_SIZE} ROWS ONLY
      `;
      
      const result = await mssqlPool.request().query(selectQuery);
      const rows = result.recordset;
      
      if (rows.length === 0) break;
      
      // Build INSERT statement with parameterized values
      const client = await pgPool.connect();
      
      try {
        await client.query('BEGIN');
        
        for (const row of rows) {
          const values = [];
          const placeholders = [];
          let paramIndex = 1;
          
          for (const col of columns) {
            let value = row[col.name];
            
            // Handle special type conversions
            if (value !== null && value !== undefined) {
              // Convert Buffer to hex string for bytea
              if (Buffer.isBuffer(value)) {
                value = '\\x' + value.toString('hex');
              }
              // Convert Date objects
              else if (value instanceof Date) {
                value = value.toISOString();
              }
              // Handle boolean conversion from bit
              else if (col.dataType.toLowerCase() === 'bit') {
                value = value === true || value === 1;
              }
            }
            
            values.push(value);
            placeholders.push(`$${paramIndex}`);
            paramIndex++;
          }
          
          const insertQuery = `INSERT INTO ${pgTableName} (${insertColumns}) VALUES (${placeholders.join(', ')})`;
          await client.query(insertQuery, values);
        }
        
        await client.query('COMMIT');
        totalMigrated += rows.length;
        
        logger.migrationProgress(fullTableName, totalMigrated, totalRows, batchNumber);
        
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
      
      offset += BATCH_SIZE;
    }
    
    logger.success('DATA_MIGRATION', `Successfully migrated all data for table "${fullTableName}"`, {
      'Total Rows': totalMigrated
    });
    
    stats.totalRowsMigrated += totalMigrated;
    return true;
    
  } catch (error) {
    logger.error('DATA_MIGRATION', `Failed to migrate data for table "${fullTableName}": ${error.message}`);
    stats.errors++;
    return false;
  }
}

/**
 * Reset sequences for serial columns after data migration
 */
async function resetSequences(schemaName, tableName, columns) {
  const fullTableName = `${schemaName}_${tableName}`;
  
  for (const col of columns) {
    if (col.isIdentity) {
      try {
        // Get the max value from the column
        const maxQuery = `SELECT COALESCE(MAX(${escapeIdentifier(col.name)}), 0) as max_val FROM ${escapeIdentifier(fullTableName)}`;
        const result = await pgPool.query(maxQuery);
        const maxVal = result.rows[0].max_val;
        
        // Reset the sequence
        const seqName = `${fullTableName}_${col.name}_seq`;
        const resetQuery = `SELECT setval(pg_get_serial_sequence('${fullTableName}', '${col.name}'), GREATEST(${maxVal}, 1), ${maxVal > 0})`;
        
        await pgPool.query(resetQuery);
        logger.info('SEQUENCE_RESET', `Reset sequence for "${fullTableName}.${col.name}" to ${maxVal}`);
      } catch (error) {
        logger.warning('SEQUENCE_RESET', `Could not reset sequence for "${fullTableName}.${col.name}": ${error.message}`);
        stats.warnings++;
      }
    }
  }
}

/**
 * Main migration function
 */
async function migrate() {
  logger = new MigrationLogger();
  
  logger.section('MIGRATION STARTED');
  logger.info('STARTUP', 'MSSQL to PostgreSQL Migration Script initialized', {
    'Source Database': mssqlConfig.database,
    'Target Database': pgConfig.database,
    'Batch Size': BATCH_SIZE
  });
  
  try {
    // Initialize connections
    await initializeConnections();
    
    // Get all tables
    const tables = await getTables();
    stats.tablesProcessed = tables.length;
    
    // Store table metadata for later use
    const tableMetadata = new Map();
    
    // Phase 1: Create all tables (without foreign keys)
    logger.section('PHASE 1: TABLE CREATION');
    logger.info('PHASE', 'Creating tables with columns, primary keys, and unique constraints...');
    
    for (const table of tables) {
      const columns = await getColumns(table.schema_name, table.table_name);
      const primaryKey = await getPrimaryKey(table.schema_name, table.table_name);
      const uniqueConstraints = await getUniqueConstraints(table.schema_name, table.table_name);
      
      tableMetadata.set(`${table.schema_name}.${table.table_name}`, {
        columns,
        primaryKey,
        uniqueConstraints
      });
      
      await createTable(table.schema_name, table.table_name, columns, primaryKey, uniqueConstraints);
    }
    
    // Phase 2: Create foreign keys
    logger.section('PHASE 2: FOREIGN KEY CREATION');
    const foreignKeys = await getAllForeignKeys();
    await createForeignKeys(foreignKeys);
    
    // Phase 3: Migrate data
    logger.section('PHASE 3: DATA MIGRATION');
    logger.info('PHASE', 'Starting data migration for all tables...');
    
    // Sort tables by foreign key dependencies to avoid constraint violations
    // Tables with no foreign keys first, then tables that reference them
    const sortedTables = sortTablesByDependencies(tables, foreignKeys);
    
    for (const table of sortedTables) {
      const metadata = tableMetadata.get(`${table.schema_name}.${table.table_name}`);
      await migrateTableData(table.schema_name, table.table_name, metadata.columns);
      await resetSequences(table.schema_name, table.table_name, metadata.columns);
    }
    
    // Write summary
    logger.writeSummary(stats);
    
    logger.success('MIGRATION', 'Migration completed successfully!', {
      'Log File': logger.getLogFilePath()
    });
    
  } catch (error) {
    logger.error('MIGRATION', `Migration failed with error: ${error.message}`, {
      'Stack': error.stack
    });
    stats.errors++;
    logger.writeSummary(stats);
    throw error;
  } finally {
    // Close connections
    logger.section('CLEANUP');
    
    if (mssqlPool) {
      await mssqlPool.close();
      logger.info('CLEANUP', 'MSSQL connection closed');
    }
    
    if (pgPool) {
      await pgPool.end();
      logger.info('CLEANUP', 'PostgreSQL connection closed');
    }
    
    logger.close();
  }
}

/**
 * Sort tables by foreign key dependencies
 * Tables with no dependencies come first
 */
function sortTablesByDependencies(tables, foreignKeys) {
  const tableNames = new Set(tables.map(t => `${t.schema_name}.${t.table_name}`));
  const dependencies = new Map();
  
  // Initialize dependencies
  for (const table of tables) {
    const fullName = `${table.schema_name}.${table.table_name}`;
    dependencies.set(fullName, new Set());
  }
  
  // Build dependency graph
  for (const fk of foreignKeys) {
    const source = `${fk.tableSchema}.${fk.tableName}`;
    const target = `${fk.referencedSchema}.${fk.referencedTable}`;
    
    if (tableNames.has(source) && tableNames.has(target) && source !== target) {
      dependencies.get(source).add(target);
    }
  }
  
  // Topological sort
  const sorted = [];
  const visited = new Set();
  const visiting = new Set();
  
  function visit(tableName) {
    if (visited.has(tableName)) return;
    if (visiting.has(tableName)) {
      // Circular dependency - just add it
      logger.warning('DEPENDENCY', `Circular dependency detected involving table "${tableName}"`);
      stats.warnings++;
      return;
    }
    
    visiting.add(tableName);
    
    const deps = dependencies.get(tableName) || new Set();
    for (const dep of deps) {
      visit(dep);
    }
    
    visiting.delete(tableName);
    visited.add(tableName);
    sorted.push(tableName);
  }
  
  for (const table of tables) {
    visit(`${table.schema_name}.${table.table_name}`);
  }
  
  // Map back to table objects
  return sorted.map(name => {
    const [schema, table] = name.split('.');
    return { schema_name: schema, table_name: table };
  });
}

// Run migration
migrate()
  .then(() => {
    console.log('\n✅ Migration completed. Check the logs directory for detailed logs.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Migration failed:', error.message);
    process.exit(1);
  });
