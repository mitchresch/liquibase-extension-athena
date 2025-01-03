package liquibase.ext.athena.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.athena.database.AthenaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.exception.ValidationErrors;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.structure.core.Table;
import liquibase.structure.core.Relation;
import liquibase.ext.athena.configuration.AthenaConfiguration;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;

public class CreateDatabaseChangeLogLockTableGeneratorAthena extends CreateDatabaseChangeLogLockTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof AthenaDatabase;
    }

    @Override
    public ValidationErrors validate(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
        database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);

        try {
            String tablePath = AthenaConfiguration.getS3TableLocation() + "/" + database.getDatabaseChangeLogLockTableName() + "/";
            return new Sql[]{
                new UnparsedSql("CREATE TABLE IF NOT EXISTS " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName()) +
                    "(ID INT, LOCKED BOOLEAN, LOCKGRANTED TIMESTAMP, LOCKEDBY STRING) LOCATION '" + tablePath +
                    "' TBLPROPERTIES ( 'table_type' = 'ICEBERG')", getAffectedTable(database)
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