package org.folio.inventory.common.domain;

public class Success<T> {
  public Success(T result) {
    this.result = result;
  }

  public T getResult() {
    return result;
  }

  private final T result;
}
