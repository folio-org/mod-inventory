package org.folio.inventory.storage.external;

public final class Limit {
  private final int limit;

  private Limit(int limit) {
    this.limit = limit;
  }

  public static Limit limit(int limit) {
    return new Limit(limit);
  }

  public int getLimit() {
    return limit;
  }
}
