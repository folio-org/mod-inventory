package org.folio.inventory.validation.exceptions;

import org.folio.inventory.support.http.server.ValidationError;

public class UnprocessableEntityException extends Exception {
  private final ValidationError validationError;

  public UnprocessableEntityException(ValidationError validationError) {
    this.validationError = validationError;
  }

  public ValidationError getValidationError() {
    return validationError;
  }
}
