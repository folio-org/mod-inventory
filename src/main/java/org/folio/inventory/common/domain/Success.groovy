package org.folio.inventory.common.domain

class Success<T> {

  private final T result

  def Success(T result) {
    this.result = result
  }

  T getResult() {
    result
  }
}
