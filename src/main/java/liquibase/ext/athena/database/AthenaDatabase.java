package liquibase.ext.athena.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.exception.DatabaseException;
import liquibase.structure.DatabaseObject;

import java.util.Arrays;
import java.util.Locale;

public class AthenaDatabase extends AbstractJdbcDatabase {

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
    public String correctObjectName(String name, Class<? extends DatabaseObject> objectType) {
        return name.toLowerCase(Locale.US)
    }

    @Override
    public boolean createsIndexesForForeignKeys() {
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
    protected String getDefaultDatabaseProductName() {
        return "AWS.Athena";
    }

    @Override
    public String getDefaultDriver(String s) {
        return "com.amazon.athena.jdbc.AthenaDriver";
    }

    @Override
    public Integer getDefaultPort() {
        return 443;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.LOWER_CASE;
    }

    @Override
    public String getShortName() {
        return "athena";
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
    public boolean isCorrectDatabaseImplementation(DatabaseConnection databaseConnection) throws DatabaseException {
        return databaseConnection.getURL().startsWith("jdbc:athena");
    }

    @Override
    public boolean isReservedWord(String name) {
        return reservedWords.contains(name.toUpperCase());
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
    public boolean supports(Class<? extends DatabaseObject> type) {
        String typeName = type.getObjectTypeName()
        if (typeName == "Table" || typeName == "Column" || typeName == "View" || typeName == "Schema") {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
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
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsForeignKeyDisable() {
        return false;
    }

    @Override
    public supportsInitiallyDeferrableColumns() {
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
    public boolean supportsTablespaces() {
        return false;
    }

    public String getS3Location() {
        return AthenaConfiguration.LIQUIBASE_S3_LOCATION.getCurrentValue();
    }
}
