package org.folio.inventory.consortium.exceptions;

import lombok.Getter;
import org.folio.inventory.common.domain.Failure;

@Getter
public class StorageOperationException extends RuntimeException {

  private final Integer statusCode;

  public StorageOperationException(Failure failure) {
    super(failure.reason());
    this.statusCode = failure.statusCode();
  }

  public StorageOperationException(String message, Integer statusCode) {
    super(message);
    this.statusCode = statusCode;
  }
}
