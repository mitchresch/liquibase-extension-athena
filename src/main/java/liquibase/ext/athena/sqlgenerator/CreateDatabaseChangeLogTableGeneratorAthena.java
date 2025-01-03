package liquibase.ext.athena.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.athena.database.AthenaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.exception.ValidationErrors;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.structure.core.Table;
import liquibase.structure.core.Relation;
import liquibase.ext.athena.configuration.AthenaConfiguration;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;

public class CreateDatabaseChangeLogTableGeneratorAthena extends CreateDatabaseChangeLogTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return database instanceof AthenaDatabase;
    }

    @Override
    public ValidationErrors validate(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
        database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        
        try {
            StringBuilder buffer = new StringBuilder();

            // CREATE TABLE table_name ...
            String tablePath = removeLastCharacter(AthenaConfiguration.getS3TableLocation(), "/") + "/" + database.getDatabaseChangeLogTableName() + "/";
            buffer.append("CREATE TABLE IF NOT EXISTS ");
            buffer.append(database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName()));
            buffer.append(" (ID STRING, AUTHOR STRING, FILENAME STRING, DATEEXECUTED TIMESTAMP, ORDEREXECUTED INT, EXECTYPE STRING," +
                    "MD5SUM STRING, DESCRIPTION STRING, COMMENTS STRING, TAG STRING, LIQUIBASE STRING," +
                    "CONTEXTS STRING, LABELS STRING, DEPLOYMENT_ID STRING)");
            buffer.append(" LOCATION '" + tablePath + "'");
            buffer.append(" TBLPROPERTIES ( 'table_type' = 'ICEBERG')");
            
            String sql = buffer.toString().replaceFirst(",\\s*$", "");

            return new Sql[]{
                new UnparsedSql(sql, getAffectedTable(database))
            };
        } finally {
            database.setObjectQuotingStrategy(currentStrategy);
        }
    }

    protected Relation getAffectedTable(Database database) {
        return new Table().setName(database.getDatabaseChangeLogTableName()).setSchema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName());
    }

    protected String removeLastCharacter(String string, String character) {
        if (!string.isEmpty() && string.charAt(string.length() - 1) == '/') {
            string = string.substring(0, string.length() - 1);
        }
        return string;
    }
}