package org.folio.inventory.exceptions;

public class UnexpectedJsonObjectException extends Exception {

  public UnexpectedJsonObjectException() {
    super();
  }

  public UnexpectedJsonObjectException(String message) {
    super(message);
  }
}
