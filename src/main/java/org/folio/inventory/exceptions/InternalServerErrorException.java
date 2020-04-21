package org.folio.inventory.exceptions;

public class InternalServerErrorException extends AbstractInventoryException {
  public InternalServerErrorException(Throwable ex) {
    super(ex.getMessage());
  }

  public InternalServerErrorException(String reason) {
    super(reason);
  }
}
