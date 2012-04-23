package ru.codeinside.pgliquibase;

import liquibase.exception.LiquibaseException;

public interface Query {
    void execute(Db db) throws LiquibaseException;
}
