package liquibase.ext.athena.database;

import liquibase.database.AbstractJdbcDatabase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;


public class AthenaDatabase extends AbstractJdbcDatabase {
    private static final String PRODUCT_NAME = "AWS.Athena";

    /**
     * JDBC Driver Info
     */
    private static final String JDBC_PREFIX = "jdbc:athena";
    private static final String DEFAULT_JDBC_DRIVER = "com.amazon.athena.jdbc.AthenaDriver";
    private static final int DEFAULT_PORT = 443;

    private Set<String> athenaReservedWords = new HashSet<String>();

    public AthenaDatabase() {
        super.setCurrentDateTimeFunction("CURRENT_TIMESTAMP");

        /**
         * Keywords taken from here:
         * https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html#list-of-ddl-reserved-words
         */
        reservedWords.addAll(Arrays.asList("ALL", "ALTER", "AND", "ARRAY", "AS", 
            "AUTHORIZATION", "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH",
            "BY", "CASE", "CASHE", "CAST", "CHAR", "COLUMN", "CONF", "CONSTRAINT",
            "COMMIT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", 
            "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DAYOFWEEK", 
            "DECIMAL", "DELETE", "DESCRIBE", "DISTINCT", "DIV", "DOUBLE","DROP",
            "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED", "EXTERNAL", "EXTRACT",
            "FALSE", "FETCH", "FLOAT", "FOLLOWING", "FOR", "FOREIGN", "FROM",
            "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT",
            "IN", "INNER", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL",
            "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL",
            "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "NUMERIC", "OF", "ON",
            "ONLY", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION",
            "PERCENT", "PRECEDING", "PRECISION", "PRESERVE", "PRIMARY", "PROCEDURE",
            "RANGE", "READS", "REDUCE", "REGEXP", "REFERENCES", "REVOKE", "RIGHT", "RLIKE", 
            "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT", "START",
            "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRANSFORM",
            "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED", "UNION", "UNIQUEJOIN", "UPDATE",
            "USER", "USING", "UTC_TIMESTAMP", "VALUES", "VARCHAR", "VIEWS", "WHEN",
            "WHERE", "WINDOW", "WITH"
        ))

        super.unquotedObjectsAreUppercased = false;
    }
    @Override
    public String getShortName() {
        return "athena";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "AWS.Athena";
    }

    @Override
    public Integer getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {

    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getURL().startsWith(JDBC_PREFIX);
    }

    @Override
    public String getDefaultDriver(String url) {
        if (String.valueOf(url).startsWith(JDBC_PREFIX)) {
            return DEFAULT_JDBC_DRIVER;
        }
        return null;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        return super.getDatabaseChangeLogTableName().toLowerCase(Locale.US);
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        return super.getDatabaseChangeLogLockTableName().toLowerCase(Locale.US);
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);
    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        return super.isSystemObject(example);
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    /**
     * This has special case logic to handle NOT quoting column names if they are
     * of type 'LiquibaseColumn' - columns in the DATABASECHANGELOG or DATABASECHANGELOGLOCK
     * tables.
     */
    @Override
    public String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        if (objectName != null) {
            objectName = objectName.trim().toLowerCase(Locale.US)
            if (mustQuoteObjectName(objectName, objectType)) {
                return quoteObject(objectName, objectType)
            } else if (quotingStrategy == ObjectQuotingStrategy.QUOTE_ALL_OBJECTS) {
                return quoteObject(objectName, objectType)
            }
        }
        return objectName;
    }

    @Override
    public boolean isReservedWord(String tableName) {
        return reservedWords.contains(tableName.toUpperCase());
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawCallStatement("select current_schema()");
    }

    @Override
    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.LOWER_CASE;
    }

    @Override
    public void setDefaultCatalogName(String defaultCatalogName) {
        if (StringUtil.isNotEmpty(defaultCatalogName)) {
            Scope.getCurrentScope().getUI().sendMessage("INFO: Setting Default Catalog.");
        }
        super.setDefaultCatalogName(defaultCatalogName);
    }

    @Override
    public boolean supportsCreateIfNotExists(Class<? extends DatabaseObject> type) {
        return true;
    }

    @Override
    public boolean supportsDatabaseChangeLogHistory() {
        return true;
    }

    @Override
    public boolean createsIndexesForForeignKeys() {
        return false;
    }

    @Override
    public String getDefaultSchemaName() {
        return null;
    }

    @Override
    public boolean isAutoCommit() {
        return true;
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public boolean requiresExplicitNullForColumns() {
        return false;
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }

    @Override
    public boolean requiresUsername() {
        return false;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsForeignKeyDisable() {
        return false;
    }

    @Override
    public boolean supportsNotNullConstraintNames() {
        return false;
    }

    @Override
    public boolean supportsPrimaryKeyNames() {
        return false;
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    public String getS3Location() {
        return AthenaConfiguration.LIQUIBASE_S3_LOCATION.getCurrentValue();
    }
}