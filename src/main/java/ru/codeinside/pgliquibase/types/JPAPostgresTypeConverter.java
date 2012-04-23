package ru.codeinside.pgliquibase.types;

import liquibase.database.structure.type.DateTimeType;
import liquibase.database.typeconversion.core.PostgresTypeConverter;

public class JPAPostgresTypeConverter extends PostgresTypeConverter {

  @Override
  public int getPriority() {
    return 10;
  }

  @Override
  public DateTimeType getDateTimeType() {
    return new DateTimeType("TIMESTAMP");
  }

}
