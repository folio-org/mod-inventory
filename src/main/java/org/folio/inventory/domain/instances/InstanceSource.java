package org.folio.inventory.domain.instances;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum InstanceSource {

  FOLIO("FOLIO"),

  MARC("MARC"),

  CONSORTIUM_FOLIO("CONSORTIUM-FOLIO"),

  CONSORTIUM_MARC("CONSORTIUM-MARC");

  private String value;

  InstanceSource(String value) {
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
  public static InstanceSource fromValue(String value) {
    for (InstanceSource b : InstanceSource.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }

}
