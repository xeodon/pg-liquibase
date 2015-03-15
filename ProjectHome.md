# Liquibase utility for PostgreSQL #

Database cleaning, schema and version comparing, **single transaction** migrations, JPA or Hibernate migrations verification. [Version 1.0](http://pg-liquibase.googlecode.com/files/pg-liquibase-1.0.jar)

## Samples ##
In samples we store [migrations](http://www.liquibase.org/manual/refactoring_commands) in db/pg/changelog.xml. Application version is `<dbVersion>_<appRevision>`, for example 1.7\_2 is database version 1.7 with second application revision.

### Clean experimental database ###
```
Utils utils = new Utils(new File("db/pg"));
utils.drop(new DbConfig("user", "pass", "jdbc:postgresql:experimental"));
```

### Apply migrations for experimental database ###
```
Utils utils = new Utils(new File("db/pg"));
DbConfig experimental = new DbConfig("user", "pass", "jdbc:postgresql:experimental");
String version = "1.7_2";
utils.createExperimental(version, experimental);
```

### Validate migrations ###
```
Utils utils = new Utils(new File("db/pg"));
DbConfig staging = new DbConfig("user", "pass", "jdbc:postgresql:staging");
DbConfig experimental = new DbConfig("user", "pass", "jdbc:postgresql:experimental");
utils.validate(staging, experimental);
```

### Apply migrations in single transaction ###
```
Utils utils = new Utils(new File("db/pg"));
DbConfig production = new DbConfig("user", "pass", "jdbc:postgresql:production");
DbConfig experimental = new DbConfig("user", "pass", "jdbc:postgresql:experimental");
String toVersion = "1.7_2";
utils.migrate(toVersion, experimental, production);
```