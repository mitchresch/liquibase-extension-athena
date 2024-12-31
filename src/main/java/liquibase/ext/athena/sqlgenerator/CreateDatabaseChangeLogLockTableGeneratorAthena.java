package liquibase.ext.athena.sqlgenerator;

import liquibase.ext.athena.change.CreateTableChangeAthena;

public class CreateDatabaseChangeLogLockTableGeneratorAthena extends AbstractSqlGenerator<CreateDatabaseChangeLogLockTableStatement> {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return database instanceof AthenaDatabase;
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
            return new Sql[]{
                new UnparsedSql("CREATE TABLE IF NOT EXISTS " + database.escapeTableName(database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName()) +
                    "(ID STRING, AUTHOR STRING, FILENAME STRING, DATEEXECUTED TIMESTAMP, ORDEREXECUTED INT, EXECTYPE STRING," +
                    "MD5SUM STRING, DESCRIPTION STRING, DESCRIPTION STRING, COMMENTS STRING, TAG STRING, LIQUIBASE STRING," +
                    "COMMENTS STRING, LABELS STRING, DEPLOYMENT_ID STRING) LOCATION " + database.getS3Location +
                    " TBLPROPERTIES ( 'table_type = 'ICEBERG')"
                )
            };
        }
    }
}