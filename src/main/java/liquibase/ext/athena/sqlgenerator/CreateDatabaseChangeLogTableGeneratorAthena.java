// package liquibase.ext.athena.sqlgenerator;

// public class CreateDatabaseChangeLogTableGeneratorAthena extends AbstractSqlGenerator<CreateDatabaseChangeLogTableStatement> {
//     @Override
//     public int getPriority() {
//         return PRIORITY_DATABASE;
//     }

//     @Override
//     public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
//         return database instanceof AthenaDatabase;
//     }

//     @Override
//     public ValidationErrors validate(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
//         return new ValidationErrors();
//     }

//     @Override
//     public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
//         ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
//         database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
//         try {
//             return new Sql[]{
//                 new UnparsedSql("CREATE TABLE IF NOT EXISTS " + database.escapeTableName(database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName()) +
//                     "(ID STRING, AUTHOR STRING, FILENAME STRING, DATEEXECUTED TIMESTAMP, ORDEREXECUTED INT, EXECTYPE STRING," +
//                     "MD5SUM STRING, DESCRIPTION STRING, DESCRIPTION STRING, COMMENTS STRING, TAG STRING, LIQUIBASE STRING," +
//                     "COMMENTS STRING, LABELS STRING, DEPLOYMENT_ID STRING) LOCATION " + database.getS3Location +
//                     " TBLPROPERTIES ( 'table_type = 'ICEBERG')"
//                 )
//             };
//         }
//     }
// }