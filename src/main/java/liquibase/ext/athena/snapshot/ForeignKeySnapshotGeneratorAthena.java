package liquibase.ext.athena.snapshot;

import liquibase.database.Database;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;

public class ForeignKeySnapshotGeneratorAthena extends ForeignKeySnapshotGenerator {
    
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        return PRIORITY_DATABASE;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {

    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        return null;
    }
}