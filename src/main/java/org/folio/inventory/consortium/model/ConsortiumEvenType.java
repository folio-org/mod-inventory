package org.folio.inventory.consortium.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ConsortiumEvenType {
  CONSORTIUM_INSTANCE_SHARING_INIT("CONSORTIUM_INSTANCE_SHARING_INIT"),
  CONSORTIUM_INSTANCE_SHARING_COMPLETE("CONSORTIUM_INSTANCE_SHARING_COMPLETE");
  private final String value;

  private ConsortiumEvenType(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return this.value;
  }

  @JsonValue
  public String value() {
    return this.value;
  }

}
