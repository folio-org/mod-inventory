package org.folio.inventory.exceptions;

public class InternalServerErrorException extends AbstractInventoryException {
  public InternalServerErrorException(String reason) {
    super(reason);
  }
}
