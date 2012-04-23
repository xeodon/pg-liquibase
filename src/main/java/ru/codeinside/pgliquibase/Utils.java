package ru.codeinside.pgliquibase;

import liquibase.database.Database;
import liquibase.diff.Diff;
import liquibase.diff.DiffResult;
import liquibase.exception.LiquibaseException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

final public class Utils {

  private Source lazySource;
  private final File changeLogDir;

  public Utils(File changeLogDir) {
    if (changeLogDir == null) {
      throw new NullPointerException();
    }
    this.changeLogDir = changeLogDir;
  }

  private Source getSource() throws LiquibaseException {
    if (lazySource == null) {
      lazySource = Source.create(changeLogDir);
    }
    return lazySource;
  }

  public void drop(DbConfig config) throws LiquibaseException, IOException {
    dropAndLoad(config, null);
  }

  public void dropAndLoad(DbConfig config, final String sqlScript) throws LiquibaseException, IOException {
    Db.execute(config, new Query() {
      public void execute(Db db) throws LiquibaseException {
        db.dropAndLoad(sqlScript);
      }
    });
  }

  public void createExperimental(String appVersion, DbConfig config) throws LiquibaseException {
    Source source = getSource();
    final Changes changes = source.getChanges(null, source.getVersion(appVersion));
    Db.execute(config, new Query() {
      public void execute(Db db) throws LiquibaseException {
        db.createTo(changes);
      }
    });
  }

  public void validate(DbConfig a, final DbConfig b) throws LiquibaseException, IOException, ParserConfigurationException {
    Db.execute(a, new Query() {
      public void execute(final Db db1) throws LiquibaseException {
        Db.execute(b, new Query() {
          public void execute(Db db2) throws LiquibaseException {
            assertEquals(db1, db2);
          }
        });
      }
    });
  }


  public void validateHibernate(String appVersion, final DbConfig hibernateCfg, DbConfig expCfg) throws LiquibaseException, IOException, ParserConfigurationException {
    final Source source = getSource();
    final Changes changes = source.getChanges(null, source.getVersion(appVersion));
    Db.execute(expCfg, new Query() {
      public void execute(final Db experimental) throws LiquibaseException {
        experimental.createTo(changes);
        Db.execute(hibernateCfg, new Query() {
          public void execute(Db hibernate) throws LiquibaseException {
            assertEquals(experimental, hibernate);
          }
        });
      }
    });
  }

  public void migrate(String grailsAppVersion, DbConfig experimentalConfig, final DbConfig targetConfig) throws LiquibaseException, IOException, ParserConfigurationException {
    final Source source = getSource();
    final String toVersion = source.getVersion(grailsAppVersion);
    final AtomicInteger status = new AtomicInteger(-1);
    Db.execute(experimentalConfig, new Query() {
      public void execute(final Db experimental) throws LiquibaseException {
        Db.execute(targetConfig, new Query() {
          public void execute(Db targetDb) throws LiquibaseException {
            targetDb.waitForLock();
            try {
              String fromVersion = source.getVersion(targetDb, toVersion);

              System.out.println("-----------------------------");
              System.out.println("From version " + fromVersion);
              System.out.println("------------------------------");
              if (fromVersion != null) {
                experimental.createTo(source.getChanges(null, fromVersion));
              } else {
                experimental.dropAndLoad(null);
              }
              final int errorCode = getEqualsStatus(targetDb, experimental, "Changes in changelog, that missed in version " + fromVersion);
              status.set(errorCode);
              if (errorCode == 0) {
                System.out.println("-----------------------------");
                System.out.println("To version " + toVersion);
                System.out.println("------------------------------");
                targetDb.migrateTo(source.getChanges(fromVersion, toVersion));
              }
            } finally {
              targetDb.releaseLock();
            }
          }
        });
      }
    });
    System.exit(status.get());
  }

  static int getEqualsStatus(Database from, Database to, String message) throws LiquibaseException {
    return isEquals(from, to, message) ? 0 : 12;
  }

  static void assertEquals(Database from, Database to) throws LiquibaseException {
    if (!isEquals(from, to, "Changes for migrate from " + from.getConnection().getCatalog() + " to " + to.getConnection().getCatalog())) {
      System.out.println();
      System.out.println("[  " + from.getConnection().getCatalog() + " != " + to.getConnection().getCatalog() + "  ]");
      System.out.println();
      System.exit(12);
    }
  }


  private static boolean isEquals(Database from, Database to, String message) throws LiquibaseException {
    DiffResult diffResult = new Diff(from, to).compare();
    try {
      if (diffResult.differencesFound()) {
        System.out.println();
        System.out.println();
        System.out.println("There is DIFFERENCE");
        System.out.println("\tfrom " + from);
        System.out.println("\tto   " + to);
        diffResult.printResult(System.out);
        System.out.println();
        System.out.println();
        System.out.println(message);
        System.out.println();
        diffResult.printChangeLog(System.out, from);
        System.out.println();
        return false;
      }
      return true;
    } catch (IOException e) {
      throw new LiquibaseException(e);
    } catch (ParserConfigurationException e) {
      throw new LiquibaseException(e);
    }
  }


}

