/**
 * MSSQL to PostgreSQL Data Type Mapper
 * Handles comprehensive type conversion between SQL Server and PostgreSQL
 */

const TYPE_MAPPINGS = {
  // Exact Numerics
  'bigint': 'BIGINT',
  'int': 'INTEGER',
  'smallint': 'SMALLINT',
  'tinyint': 'SMALLINT',
  'bit': 'BOOLEAN',
  'decimal': 'DECIMAL',
  'numeric': 'NUMERIC',
  'money': 'DECIMAL(19,4)',
  'smallmoney': 'DECIMAL(10,4)',

  // Approximate Numerics
  'float': 'DOUBLE PRECISION',
  'real': 'REAL',

  // Date and Time
  'date': 'DATE',
  'datetime': 'TIMESTAMP',
  'datetime2': 'TIMESTAMP',
  'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
  'smalldatetime': 'TIMESTAMP',
  'time': 'TIME',

  // Character Strings
  'char': 'CHAR',
  'varchar': 'VARCHAR',
  'text': 'TEXT',

  // Unicode Character Strings
  'nchar': 'CHAR',
  'nvarchar': 'VARCHAR',
  'ntext': 'TEXT',

  // Binary Strings
  'binary': 'BYTEA',
  'varbinary': 'BYTEA',
  'image': 'BYTEA',

  // Other Data Types
  'uniqueidentifier': 'UUID',
  'xml': 'XML',
  'sql_variant': 'TEXT',
  'hierarchyid': 'TEXT',
  'geometry': 'TEXT',
  'geography': 'TEXT',
  'rowversion': 'BYTEA',
  'timestamp': 'BYTEA',

  // CLR Types
  'sysname': 'VARCHAR(128)'
};

/**
 * Maps MSSQL data type to PostgreSQL equivalent
 * @param {string} mssqlType - The MSSQL data type name
 * @param {number|null} maxLength - Maximum length for string types
 * @param {number|null} precision - Precision for numeric types
 * @param {number|null} scale - Scale for numeric types
 * @returns {string} PostgreSQL data type
 */
function mapDataType(mssqlType, maxLength = null, precision = null, scale = null) {
  const typeLower = mssqlType.toLowerCase().trim();
  
  // Handle special cases first
  
  // VARCHAR(MAX), NVARCHAR(MAX), VARBINARY(MAX)
  if ((typeLower === 'varchar' || typeLower === 'nvarchar') && maxLength === -1) {
    return 'TEXT';
  }
  
  if (typeLower === 'varbinary' && maxLength === -1) {
    return 'BYTEA';
  }

  // Handle types with precision and scale
  if (typeLower === 'decimal' || typeLower === 'numeric') {
    if (precision !== null && scale !== null) {
      return `NUMERIC(${precision},${scale})`;
    } else if (precision !== null) {
      return `NUMERIC(${precision})`;
    }
    return 'NUMERIC';
  }

  // Handle character types with length
  if (typeLower === 'char' || typeLower === 'nchar') {
    if (maxLength && maxLength > 0) {
      // NCHAR stores unicode, so actual length is maxLength/2 in MSSQL
      const length = typeLower === 'nchar' ? Math.floor(maxLength / 2) : maxLength;
      return `CHAR(${length})`;
    }
    return 'CHAR(1)';
  }

  if (typeLower === 'varchar' || typeLower === 'nvarchar') {
    if (maxLength && maxLength > 0) {
      // NVARCHAR stores unicode, so actual length is maxLength/2 in MSSQL
      const length = typeLower === 'nvarchar' ? Math.floor(maxLength / 2) : maxLength;
      return `VARCHAR(${length})`;
    }
    return 'VARCHAR';
  }

  // Handle binary types with length
  if (typeLower === 'binary' || typeLower === 'varbinary') {
    return 'BYTEA';
  }

  // Handle float with precision
  if (typeLower === 'float') {
    if (precision !== null && precision <= 24) {
      return 'REAL';
    }
    return 'DOUBLE PRECISION';
  }

  // Handle datetime2 with precision
  if (typeLower === 'datetime2') {
    if (scale !== null && scale >= 0 && scale <= 6) {
      return `TIMESTAMP(${scale})`;
    }
    return 'TIMESTAMP';
  }

  // Handle time with precision
  if (typeLower === 'time') {
    if (scale !== null && scale >= 0 && scale <= 6) {
      return `TIME(${scale})`;
    }
    return 'TIME';
  }

  // Handle datetimeoffset with precision
  if (typeLower === 'datetimeoffset') {
    if (scale !== null && scale >= 0 && scale <= 6) {
      return `TIMESTAMP(${scale}) WITH TIME ZONE`;
    }
    return 'TIMESTAMP WITH TIME ZONE';
  }

  // Default mapping
  const mappedType = TYPE_MAPPINGS[typeLower];
  if (mappedType) {
    return mappedType;
  }

  // Unknown type - default to TEXT with warning
  console.warn(`Unknown MSSQL type: ${mssqlType}, defaulting to TEXT`);
  return 'TEXT';
}

/**
 * Escapes a PostgreSQL identifier (table name, column name, etc.)
 * @param {string} identifier - The identifier to escape
 * @returns {string} Escaped identifier
 */
function escapeIdentifier(identifier) {
  // Replace any double quotes with two double quotes
  return `"${identifier.replace(/"/g, '""')}"`;
}

/**
 * Converts MSSQL default value to PostgreSQL equivalent
 * @param {string} defaultValue - The MSSQL default value
 * @param {string} dataType - The data type
 * @returns {string|null} PostgreSQL default value or null
 */
function convertDefaultValue(defaultValue, dataType) {
  if (!defaultValue) return null;
  
  // Remove outer parentheses that MSSQL adds
  let value = defaultValue.replace(/^\(+|\)+$/g, '');
  
  // Handle common MSSQL functions
  const functionMappings = {
    'getdate()': 'CURRENT_TIMESTAMP',
    'getutcdate()': 'CURRENT_TIMESTAMP AT TIME ZONE \'UTC\'',
    'sysdatetime()': 'CURRENT_TIMESTAMP',
    'sysutcdatetime()': 'CURRENT_TIMESTAMP AT TIME ZONE \'UTC\'',
    'sysdatetimeoffset()': 'CURRENT_TIMESTAMP',
    'newid()': 'gen_random_uuid()',
    'newsequentialid()': 'gen_random_uuid()',
    'user_name()': 'CURRENT_USER',
    'suser_sname()': 'CURRENT_USER',
    'host_name()': 'inet_client_addr()'
  };

  const lowerValue = value.toLowerCase();
  if (functionMappings[lowerValue]) {
    return functionMappings[lowerValue];
  }

  // Handle string literals
  if (value.startsWith("N'") || value.startsWith("'")) {
    return value.replace(/^N'/, "'");
  }

  // Handle numeric values
  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return value;
  }

  // Handle boolean values for bit type
  if (dataType.toLowerCase() === 'boolean' || dataType.toLowerCase() === 'bit') {
    if (value === '1' || value.toLowerCase() === 'true') return 'TRUE';
    if (value === '0' || value.toLowerCase() === 'false') return 'FALSE';
  }

  // Return as-is for other cases
  return value;
}

/**
 * Generates PostgreSQL column definition
 * @param {Object} column - Column metadata
 * @returns {string} PostgreSQL column definition
 */
function generateColumnDefinition(column) {
  const parts = [];
  
  // Column name
  parts.push(escapeIdentifier(column.name));
  
  // Data type
  const pgType = mapDataType(
    column.dataType,
    column.maxLength,
    column.precision,
    column.scale
  );
  parts.push(pgType);
  
  // NOT NULL constraint
  if (!column.isNullable) {
    parts.push('NOT NULL');
  }
  
  // Default value
  if (column.defaultValue) {
    const pgDefault = convertDefaultValue(column.defaultValue, pgType);
    if (pgDefault) {
      parts.push(`DEFAULT ${pgDefault}`);
    }
  }
  
  return parts.join(' ');
}

module.exports = {
  mapDataType,
  escapeIdentifier,
  convertDefaultValue,
  generateColumnDefinition,
  TYPE_MAPPINGS
};
