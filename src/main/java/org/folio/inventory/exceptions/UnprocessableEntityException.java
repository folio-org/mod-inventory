package org.folio.inventory.exceptions;

import org.folio.inventory.support.http.server.ValidationError;

public class UnprocessableEntityException extends AbstractInventoryException {
  private final String message;
  private final String propertyName;
  private final String propertyValue;

  public UnprocessableEntityException(ValidationError validationError) {
    this(validationError.message, validationError.propertyName, validationError.value);
  }

  public UnprocessableEntityException(String message, String propertyName, String propertyValue) {
    super(message);
    this.message = message;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  @Override
  public String getMessage() {
    return message;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getPropertyValue() {
    return propertyValue;
  }
}
