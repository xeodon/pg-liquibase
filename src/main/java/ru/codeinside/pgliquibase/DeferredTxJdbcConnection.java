package ru.codeinside.pgliquibase;

import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

import java.sql.Connection;

public final class DeferredTxJdbcConnection extends JdbcConnection {

  private boolean deferredCommit = false;

  public DeferredTxJdbcConnection(Connection connection) throws DatabaseException {
    super(connection);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws DatabaseException {
    if (!deferredCommit) {
      super.setAutoCommit(autoCommit);
    }
  }

  @Override
  public void commit() throws DatabaseException {
    if (!deferredCommit) {
      super.commit();
    }
  }

  @Override
  public void rollback() throws DatabaseException {
    if (!deferredCommit) {
      super.rollback();
    }
  }

  public void setDeferredCommit(boolean deferredCommit) {
    this.deferredCommit = deferredCommit;
  }
}
