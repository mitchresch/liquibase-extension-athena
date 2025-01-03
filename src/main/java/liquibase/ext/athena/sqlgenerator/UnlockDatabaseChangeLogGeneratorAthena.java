package liquibase.ext.athena.sqlgenerator;

import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.util.NetUtil;
import liquibase.ext.athena.database.AthenaDatabase;

public class UnlockDatabaseChangeLogGeneratorAthena extends UnlockDatabaseChangeLogGenerator {

    @Override
    public boolean supports(UnlockDatabaseChangeLogStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof AthenaDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(UnlockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(UnlockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();

        // use LEGACY quoting since we're dealing with system objects
        ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
        database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        try {
            String dateValue = database.getCurrentDateTimeFunction();
            UpdateStatement updateStatement = new UpdateStatement(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName());
            updateStatement.addNewColumnValue("locked", false);
            updateStatement.addNewColumnValue("lockgranted", null);
            updateStatement.addNewColumnValue("lockedby", null);
            updateStatement.setWhereClause(database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogTableName(), "ID") + " = 1");

            return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
        } finally {
            database.setObjectQuotingStrategy(currentStrategy);
        }
    }
}