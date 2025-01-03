package liquibase.ext.athena.snapshot;

import liquibase.database.Database;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.ext.athena.database.AthenaDatabase;

public class ForeignKeySnapshotGeneratorAthena extends ForeignKeySnapshotGenerator {
    
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof AthenaDatabase) {
            return super.getPriority(objectType, database);
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    public Class<? extends ForeignKeySnapshotGenerator>[] replaces() {
        return new Class[] {
            ForeignKeySnapshotGenerator.class
        };
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {

    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        return null;
    }
}
