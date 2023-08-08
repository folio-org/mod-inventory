package org.folio.inventory.dataimport.entities;

public class PartialError {
  private String id;
  private String error;
  private String holdingId;

  public PartialError(String id, String error) {
    this.id = id;
    this.error = error;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getHoldingId() {
    return holdingId;
  }

  public void setHoldingId(String holdingId) {
    this.holdingId = holdingId;
  }
}
