package liquibase.ext.athena.changelog.filter;

import liquibase.changelog.filter.ChangeSetFilter;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.ext.athena.database.AthenaDatabase;
import liquibase.changelog.filter.ChangeSetFilterResult;
import java.util.List;
import java.util.ArrayList;

public class UnsupportedChangeSetFilterAthena implements ChangeSetFilter {

    private final Database database;

    public UnsupportedChangeSetFilterAthena(Database database) {
        this.database = database;
    }

    @Override
    public ChangeSetFilterResult accepts(ChangeSet changeSet) {
        if (database instanceof AthenaDatabase) {
            List<String> changes = new ArrayList<String>();
            List<String> supportedChanges = new ArrayList<String>();
            supportedChanges.add("createTable");
            changeSet.getChanges().stream().forEach(change -> changes.add(change.toString()));
            if (supportedChanges.containsAll(changes)) {
                return new ChangeSetFilterResult(true, "Valid Athena change", this.getClass(), getMdcName(), getDisplayName());
            } else {
                return new ChangeSetFilterResult(false, "Ignoring changeset because unsupported Athena change", this.getClass(), getMdcName(), getDisplayName());
            }
            
        } else {
            return new ChangeSetFilterResult(true, "Not Athena", this.getClass(), getMdcName(), getDisplayName());
        }
    }

    @Override
    public String getMdcName() {
        return "unsupported";
    }

    @Override
    public String getDisplayName() {
        return "Unsupported";
    }
}
