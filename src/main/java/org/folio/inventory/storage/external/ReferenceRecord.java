package org.folio.inventory.storage.external;

public record ReferenceRecord(String id, String name) {

  @Override
  public String toString() {
    return id + " " + name;
  }
}
