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
        AthenaDatabase athenaDatabase = (AthenaDatabase) database;
        try {
            return new Sql[]{
                new UnparsedSql("CREATE TABLE IF NOT EXISTS " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName()) +
                    "(ID STRING, AUTHOR STRING, FILENAME STRING, DATEEXECUTED TIMESTAMP, ORDEREXECUTED INT, EXECTYPE STRING," +
                    "MD5SUM STRING, DESCRIPTION STRING, DESCRIPTION STRING, COMMENTS STRING, TAG STRING, LIQUIBASE STRING," +
                    "COMMENTS STRING, LABELS STRING, DEPLOYMENT_ID STRING) LOCATION " + AthenaConfiguration.getS3TableLocation() +
                    " TBLPROPERTIES ( 'table_type' = 'ICEBERG')", getAffectedTable(database)
                )
            };
        } finally {
            database.setObjectQuotingStrategy(currentStrategy);
        }
    }

    protected Relation getAffectedTable(Database database) {
        return new Table().setName(database.getDatabaseChangeLogTableName()).setSchema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName());
    }
}