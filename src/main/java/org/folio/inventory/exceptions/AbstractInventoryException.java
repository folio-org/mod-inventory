package org.folio.inventory.exceptions;

public abstract class AbstractInventoryException extends RuntimeException {

  public AbstractInventoryException(String message) {
    super(message);
  }
}
