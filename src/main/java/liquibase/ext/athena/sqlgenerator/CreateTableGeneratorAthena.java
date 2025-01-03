package liquibase.ext.athena.sqlgenerator;

import liquibase.ext.athena.database.AthenaDatabase;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import liquibase.ext.athena.configuration.AthenaConfiguration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.nio.file.Paths;

public class CreateTableGeneratorAthena extends CreateTableGenerator {

    /*
     * Returns true if the database is Athena
     */
    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof AthenaDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        // CREATE TABLE table_name ...
        buffer.append("CREATE TABLE ");

        if (statement.isIfNotExists() && database.supportsCreateIfNotExists(Table.class)) {
            buffer.append("IF NOT EXISTS ");
        }

        buffer.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()))
                .append(" ")
                .append("(");

        Iterator<String> columnIterator = statement.getColumns().iterator();
        List<String> primaryKeyColumns = new LinkedList<>();

        /*
         * Build the list of columns and properties in the form
         * (
         *   column1,
         *   ...,
         *   columnN
         * )
         * LOCATION <>
         * TBLPROPERTIES ( 'table_type = ICEBERG' )
         */
        while (columnIterator.hasNext()) {
            String column = columnIterator.next();

            buffer.append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), column));
            buffer.append(" ").append(statement.getColumnTypes().get(column).toDatabaseDataType(database).toSql());

            if (statement.getNotNullColumns().containsKey(column)) {
                buffer.append(" NOT NULL");
            }

            if (columnIterator.hasNext()) {
                buffer.append(", ");
            }
        }

        String tablePath = Paths.get(AthenaConfiguration.getS3TableLocation(), statement.getTableName()).toString();

        buffer.append(") LOCATION '" + tablePath + "' TBLPROPERTIES ( 'table_type' = 'ICEBERG' )");

        String sql = buffer.toString().replaceFirst(",\\s*$", "");

        return new Sql[]{new UnparsedSql(sql, new Table().setName(statement.getTableName()).setSchema(new Schema(statement.getCatalogName(), statement.getSchemaName())))};
    }
}
