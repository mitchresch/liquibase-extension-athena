package liquibase.ext.athena.sqlgenerator;

import liquibase.ext.athena.database.AthenaDatabase;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.sqlgenerator.core.AddPrimaryKeyGenerator;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.AddPrimaryKeyStatement;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.ext.athena.configuration.AthenaConfiguration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class AddPrimaryKeyGeneratorAthena extends AddPrimaryKeyGenerator {

    /*
     * Returns true if the database is Athena
     */
    @Override
    public boolean supports(AddPrimaryKeyStatement statement, Database database) {
        return false;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(AddPrimaryKeyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        String sql = buffer.toString().replaceFirst(",\\s*$", "");

        return new Sql[]{new UnparsedSql(sql, new Table().setName(statement.getTableName()).setSchema(new Schema(statement.getCatalogName(), statement.getSchemaName())))};
    }
}
