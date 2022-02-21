package org.folio.inventory.dataimport.exceptions;

public class DuplicatedEventException extends RuntimeException {

  public DuplicatedEventException(String message) {
    super(message);
  }
}
