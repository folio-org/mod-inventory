package org.folio.inventory.domain.view.request;

import java.util.Arrays;

public enum RequestStatus {
  OPEN_NOT_YET_FILLED("Open - Not yet filled"),
  OPEN_AWAITING_PICKUP("Open - Awaiting pickup"),
  OPEN_AWAITING_DELIVERY("Open - Awaiting delivery"),
  OPEN_IN_TRANSIT("Open - In transit");

  private final String value;

  RequestStatus(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Returns RequestStatus matched by given value, or null if can not match.
   *
   * @param value - Value to match.
   * @return matched RequestStatus or null.
   */
  public static RequestStatus of(String value) {
    return Arrays.stream(values())
      .filter(currentValue -> currentValue.getValue().equals(value))
      .findFirst()
      .orElse(null);
  }
}

