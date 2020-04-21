package org.folio.inventory.exceptions;

public class NotFoundException extends AbstractInventoryException {
  public NotFoundException(String message) {
    super(message);
  }
}
