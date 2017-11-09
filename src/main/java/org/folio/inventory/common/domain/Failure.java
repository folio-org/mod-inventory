package org.folio.inventory.common.domain;

public class Failure {
  public Failure(String reason, Integer statusCode) {
    this.reason = reason;
    this.statusCode = statusCode;
  }

  public final String getReason() {
    return reason;
  }

  public final Integer getStatusCode() {
    return statusCode;
  }

  private final String reason;
  private final Integer statusCode;
}
