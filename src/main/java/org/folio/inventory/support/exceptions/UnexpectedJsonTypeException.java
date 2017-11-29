package org.folio.inventory.support.exceptions;

public class UnexpectedJsonTypeException extends Exception {

  public UnexpectedJsonTypeException() {
    super();
  }

  public UnexpectedJsonTypeException(String message) {
    super(message);
  }
}
