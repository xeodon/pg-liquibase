package ru.codeinside.pgliquibase;

import liquibase.database.core.PostgresDatabase;
import liquibase.database.typeconversion.TypeConverter;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.LockService;
import liquibase.servicelocator.ServiceLocator;
import org.apache.commons.io.FileUtils;
import ru.codeinside.pgliquibase.types.JPAPostgresTypeConverter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

final class Db extends PostgresDatabase {

  private DeferredTxJdbcConnection deferredTxJdbcConnection;
  private Connection connection;

  private Db(Connection connection) throws DatabaseException {
    this.connection = connection;
    deferredTxJdbcConnection = new DeferredTxJdbcConnection(this.connection);
    setConnection(deferredTxJdbcConnection);
  }

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("PostgreSQL DataSource unable to load PostgreSQL JDBC Driver");
    }
    final ServiceLocator serviceLocator = ServiceLocator.getInstance();
    final Class<JPAPostgresTypeConverter> jpaTypeConverter = JPAPostgresTypeConverter.class;
    serviceLocator.addPackageToScan(jpaTypeConverter.getPackage().getName());
    if (jpaTypeConverter != serviceLocator.findClass(TypeConverter.class)) {
      throw new IllegalStateException("ServiceLocator already cached type converters! Can not patch PostgreSQL types!");
    }
  }

  private static Db create(DbConfig config) throws DatabaseException {
    if (!config.url.startsWith("jdbc:postgresql:")) {
      throw new DatabaseException("Invalid url " + config.url);
    }
    try {
      return new Db(DriverManager.getConnection(config.url, config.username, config.password));
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  static void execute(DbConfig config, Query query) throws LiquibaseException {
    Db db = create(config);
    try {
      query.execute(db);
    } finally {
      db.close();
    }
  }

  void createTo(Changes changes) throws LiquibaseException {
    deferredTxJdbcConnection.setDeferredCommit(true);
    try {
      dropThenLoad(null);
      checkChangeLogTable();
      changes.apply(this);
    } finally {
      deferredTxJdbcConnection.setDeferredCommit(false);
    }
    deferredTxJdbcConnection.commit();
  }

  void migrateTo(Changes changes) throws LiquibaseException {
    deferredTxJdbcConnection.setDeferredCommit(true);
    try {
      changes.apply(this);
    } finally {
      deferredTxJdbcConnection.setDeferredCommit(false);
    }
    deferredTxJdbcConnection.commit();
  }


  void dropAndLoad(String sqlScriptFileName) throws DatabaseException {
    deferredTxJdbcConnection.setDeferredCommit(true);
    try {
      dropThenLoad(sqlScriptFileName);
    } finally {
      deferredTxJdbcConnection.setDeferredCommit(false);
    }
    deferredTxJdbcConnection.commit();
  }

  private void dropThenLoad(String sqlScriptFileName) throws DatabaseException {
    try {
      dropDatabaseObjects(getDefaultSchemaName());
      dropSpecialTables();
      if (sqlScriptFileName != null) {
        load(sqlScriptFileName);
      }
    } catch (DatabaseException e) {
      throw e;
    } catch (SQLException e) {
      SQLException n = e;
      while (n != null) {
        System.err.println(e.getMessage());
        n = n.getNextException();
      }
      System.err.println();
      throw new DatabaseException(e);
    } catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void checkChangeLogTable() throws DatabaseException {
    checkDatabaseChangeLogTable(false, null, null);
    if (!LockService.getInstance(this).hasChangeLogLock()) {
      checkDatabaseChangeLogLockTable();
    }
  }

  private void dropSpecialTables() throws DatabaseException, SQLException {
    Statement statement = connection.createStatement();
    statement.execute("drop table IF EXISTS databasechangelog, databasechangeloglock");
  }

  private void load(String sqlScriptFileName) throws IOException, DatabaseException, SQLException {
    String script = FileUtils.readFileToString(new File(sqlScriptFileName), "UTF8");
    Statement statement = connection.createStatement();
    statement.execute(script);
    statement.close();
  }

  void releaseLock() throws LockException {
    begin();
    LockService.getInstance(this).releaseLock();
  }

  void waitForLock() throws LockException {
    begin();
    LockService.getInstance(this).waitForLock();
  }

  private void begin() throws LockException {
    deferredTxJdbcConnection.setDeferredCommit(false);
    try {
      deferredTxJdbcConnection.rollback();
    } catch (DatabaseException e) {
      throw new LockException(e);
    }
  }

}
