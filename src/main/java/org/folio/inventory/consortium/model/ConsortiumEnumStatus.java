package org.folio.inventory.consortium.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ConsortiumEnumStatus {
  COMPLETE("COMPLETE"),

  ERROR("ERROR");

  private String value;

  ConsortiumEnumStatus(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static ConsortiumEnumStatus fromValue(String value) {
    for (ConsortiumEnumStatus b : ConsortiumEnumStatus.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
