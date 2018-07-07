package org.folio.inventory.storage.external;

public class ReferenceRecord {
  public final String id;
  public final String name;

  public ReferenceRecord(String id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public String toString() {
    return id + " " + name;
  }
}
