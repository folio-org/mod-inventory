package org.folio.inventory.consortium.exceptions;

/**
 * Exception that used for consortium process.
 */
public class ConsortiumException extends RuntimeException {

  public ConsortiumException(String message) {
    super(message);
  }

  public ConsortiumException(String message, Throwable cause) {
    super(message, cause);
  }
}
