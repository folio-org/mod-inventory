package org.folio.inventory.domain;

public enum Messages {
  START_INGEST("org.folio.inventory.ingest.start"),
  INGEST_COMPLETED("org.folio.inventory.ingest.completed");

  Messages(String address) {
    this.Address = address;
  }

  public final String Address;
}
