package org.folio.inventory.storage.external;

import static java.lang.String.format;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public final class CqlQuery {
  private final String query;

  private CqlQuery(String query) {
    this.query = query;
  }

  public CqlQuery or(CqlQuery other) {
    return new CqlQuery(format("%s or %s", toString(), other));
  }

  public static CqlQuery exactMatchAny(String indexName, Collection<String> values) {
    final String valuesQuery = values.stream()
      .filter(Objects::nonNull)
      .map(value -> "\"" + value + "\"")
      .collect(Collectors.joining(" or "));

    return new CqlQuery(format("%s==(%s)", indexName, valuesQuery));
  }

  @Override
  public String toString() {
    return query;
  }
}
