package org.folio.inventory.validation.exceptions;

import org.folio.inventory.exceptions.AbstractInventoryException;

public class NotFoundException extends AbstractInventoryException {
  public NotFoundException(String message) {
    super(message);
  }
}
