package org.folio.inventory.domain;

public class Identifier {
  public final String identifierTypeId;
  public final String value;

  public Identifier(String identifierTypeId, String value) {
    this.identifierTypeId = identifierTypeId;
    this.value = value;
  }
}
