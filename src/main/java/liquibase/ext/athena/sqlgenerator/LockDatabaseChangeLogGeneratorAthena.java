package liquibase.ext.athena.sqlgenerator;

import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.util.NetUtil;
import liquibase.ext.athena.database.AthenaDatabase;

public class LockDatabaseChangeLogGeneratorAthena extends LockDatabaseChangeLogGenerator {

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof AthenaDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    protected static final String hostname;
    protected static final String hostaddress;
    protected static final String hostDescription = (System.getProperty("liquibase.hostDescription") == null) ? "" :
        ("#" + System.getProperty("liquibase.hostDescription"));

    static {
        try {
            hostname = NetUtil.getLocalHostName();
            hostaddress = NetUtil.getLocalHostAddress();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();

        // use LEGACY quoting since we're dealing with system objects
        ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
        database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        try {
            String dateValue = database.getCurrentDateTimeFunction();
            UpdateStatement updateStatement = new UpdateStatement(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName());
            updateStatement.addNewColumnValue("locked", true);
            updateStatement.addNewColumnValue("lockgranted", new DatabaseFunction(dateValue));
            updateStatement.addNewColumnValue("lockedby", hostname + hostDescription + " (" + hostaddress + ")");
            updateStatement.setWhereClause(database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogTableName(), "ID") + " = 1 AND " + database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogTableName(), "LOCKED") + " = "+ DataTypeFactory.getInstance().fromDescription("boolean", database).objectToSql(false, database));

            return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
        } finally {
            database.setObjectQuotingStrategy(currentStrategy);
        }
    }
}