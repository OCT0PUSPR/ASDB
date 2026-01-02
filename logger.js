const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class MigrationLogger {
  constructor() {
    this.logId = uuidv4().split('-')[0].toUpperCase();
    this.startTime = new Date();
    this.logDir = path.join(__dirname, 'logs');
    this.logFileName = this.generateLogFileName();
    this.logFilePath = path.join(this.logDir, this.logFileName);
    this.buffer = [];
    this.flushInterval = null;
    
    this.ensureLogDirectory();
    this.writeHeader();
    this.startAutoFlush();
  }

  generateLogFileName() {
    const date = this.startTime;
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    
    return `migration_${year}${month}${day}_${hours}${minutes}${seconds}_${this.logId}.txt`;
  }

  ensureLogDirectory() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  writeHeader() {
    const header = `
================================================================================
                    MSSQL TO POSTGRESQL MIGRATION LOG
================================================================================
  Log ID:           ${this.logId}
  Start Time:       ${this.formatDateTime(this.startTime)}
  Log File:         ${this.logFileName}
================================================================================

`;
    fs.writeFileSync(this.logFilePath, header);
  }

  formatDateTime(date) {
    return date.toISOString().replace('T', ' ').replace('Z', ' UTC');
  }

  formatTimestamp() {
    return new Date().toISOString().replace('T', ' ').split('.')[0];
  }

  formatMessage(level, category, message, details = null) {
    const timestamp = this.formatTimestamp();
    const levelPadded = level.padEnd(7);
    const categoryPadded = category.padEnd(20);
    
    let logLine = `[${timestamp}] [${levelPadded}] [${categoryPadded}] ${message}`;
    
    if (details) {
      if (typeof details === 'object') {
        const detailLines = Object.entries(details)
          .map(([key, value]) => `                                                    → ${key}: ${value}`)
          .join('\n');
        logLine += '\n' + detailLines;
      } else {
        logLine += `\n                                                    → ${details}`;
      }
    }
    
    return logLine;
  }

  log(level, category, message, details = null) {
    const formattedMessage = this.formatMessage(level, category, message, details);
    this.buffer.push(formattedMessage);
    
    // Console output with colors
    const colors = {
      INFO: '\x1b[36m',    // Cyan
      SUCCESS: '\x1b[32m', // Green
      WARNING: '\x1b[33m', // Yellow
      ERROR: '\x1b[31m',   // Red
      DEBUG: '\x1b[90m',   // Gray
      SECTION: '\x1b[35m'  // Magenta
    };
    const reset = '\x1b[0m';
    const color = colors[level] || reset;
    
    console.log(`${color}${formattedMessage}${reset}`);
  }

  info(category, message, details = null) {
    this.log('INFO', category, message, details);
  }

  success(category, message, details = null) {
    this.log('SUCCESS', category, message, details);
  }

  warning(category, message, details = null) {
    this.log('WARNING', category, message, details);
  }

  error(category, message, details = null) {
    this.log('ERROR', category, message, details);
  }

  debug(category, message, details = null) {
    this.log('DEBUG', category, message, details);
  }

  section(title) {
    const separator = '─'.repeat(80);
    const sectionHeader = `
${separator}
  ${title.toUpperCase()}
${separator}`;
    this.buffer.push(sectionHeader);
    console.log(`\x1b[35m${sectionHeader}\x1b[0m`);
  }

  tableInfo(tableName, action, details) {
    const message = `Table "${tableName}": ${action}`;
    this.info('TABLE_OPERATION', message, details);
  }

  columnInfo(tableName, columnName, dataType, constraints) {
    const constraintStr = constraints.length > 0 ? constraints.join(', ') : 'None';
    this.info('COLUMN_DEFINITION', `Adding column "${columnName}" to table "${tableName}"`, {
      'Data Type': dataType,
      'Constraints': constraintStr
    });
  }

  foreignKeyInfo(tableName, fkName, referencedTable, columns) {
    this.info('FOREIGN_KEY', `Creating foreign key "${fkName}" on table "${tableName}"`, {
      'Referenced Table': referencedTable,
      'Column Mapping': columns
    });
  }

  primaryKeyInfo(tableName, pkName, columns) {
    this.info('PRIMARY_KEY', `Creating primary key "${pkName}" on table "${tableName}"`, {
      'Columns': columns.join(', ')
    });
  }

  migrationProgress(tableName, rowsMigrated, totalRows, batchNumber) {
    const percentage = totalRows > 0 ? ((rowsMigrated / totalRows) * 100).toFixed(1) : 100;
    this.info('DATA_MIGRATION', `Migrating data for table "${tableName}"`, {
      'Progress': `${rowsMigrated}/${totalRows} rows (${percentage}%)`,
      'Batch': batchNumber
    });
  }

  startAutoFlush() {
    this.flushInterval = setInterval(() => this.flush(), 1000);
  }

  flush() {
    if (this.buffer.length > 0) {
      const content = this.buffer.join('\n') + '\n';
      fs.appendFileSync(this.logFilePath, content);
      this.buffer = [];
    }
  }

  writeSummary(stats) {
    const endTime = new Date();
    const duration = ((endTime - this.startTime) / 1000).toFixed(2);
    
    const summary = `

================================================================================
                           MIGRATION SUMMARY
================================================================================
  Log ID:                 ${this.logId}
  Start Time:             ${this.formatDateTime(this.startTime)}
  End Time:               ${this.formatDateTime(endTime)}
  Duration:               ${duration} seconds
--------------------------------------------------------------------------------
  Tables Processed:       ${stats.tablesProcessed}
  Tables Created:         ${stats.tablesCreated}
  Primary Keys Created:   ${stats.primaryKeysCreated}
  Foreign Keys Created:   ${stats.foreignKeysCreated}
  Total Rows Migrated:    ${stats.totalRowsMigrated}
--------------------------------------------------------------------------------
  Errors:                 ${stats.errors}
  Warnings:               ${stats.warnings}
  Status:                 ${stats.errors === 0 ? 'SUCCESS' : 'COMPLETED WITH ERRORS'}
================================================================================
`;
    
    this.buffer.push(summary);
    console.log(`\x1b[35m${summary}\x1b[0m`);
  }

  close() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }
    this.flush();
  }

  getLogFilePath() {
    return this.logFilePath;
  }
}

module.exports = MigrationLogger;
