package ru.codeinside.pgliquibase;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

import java.util.List;

final class Changes {
    final private DatabaseChangeLog databaseChangeLog;
    final private List<ChangeSet> changes;

    Changes(DatabaseChangeLog databaseChangeLog, List<ChangeSet> changes) {
        this.databaseChangeLog = databaseChangeLog;
        this.changes = changes;
    }

    void apply(Database database) throws LiquibaseException {
        for (ChangeSet item : changes) {
            database.markChangeSetExecStatus(item, item.execute(databaseChangeLog, database));
        }
    }

}
