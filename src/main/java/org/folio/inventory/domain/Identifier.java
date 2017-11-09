package org.folio.inventory.domain;

public class Identifier {
  public final String namespace;
  public final String value;

  public Identifier(String namespace, String value) {
    this.namespace = namespace;
    this.value = value;
  }
}
