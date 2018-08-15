package org.folio.inventory.domain;

public class Identifier {
  public static final String IDENTIFIER_TYPE_ID = "identifierTypeId";
  public static final String VALUE = "value";

  public final String identifierTypeId;
  public final String value;

  public Identifier(String identifierTypeId, String value) {
    this.identifierTypeId = identifierTypeId;
    this.value = value;
  }
}
