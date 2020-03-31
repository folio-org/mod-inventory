package org.folio.inventory.validation.exceptions;

import org.folio.inventory.exceptions.AbstractInventoryException;
import org.folio.inventory.support.http.server.ValidationError;

public class UnprocessableEntityException extends AbstractInventoryException {
  private final ValidationError validationError;

  public UnprocessableEntityException(ValidationError validationError) {
    super(validationError.message);
    this.validationError = validationError;
  }

  public ValidationError getValidationError() {
    return validationError;
  }
}
