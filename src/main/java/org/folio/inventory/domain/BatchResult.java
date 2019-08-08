package org.folio.inventory.domain;

import java.util.List;

public class BatchResult<T> {

  private List<T> items;

  private List<String> errorMessages;

  public List<T> getBatchItems() {
    return items;
  }

  public void setBatchItems(List<T> items) {
    this.items = items;
  }

  public List<String> getErrorMessages() {
    return errorMessages;
  }

  public void setErrorMessages(List<String> errorMessages) {
    this.errorMessages = errorMessages;
  }
}
