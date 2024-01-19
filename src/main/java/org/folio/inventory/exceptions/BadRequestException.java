package org.folio.inventory.exceptions;

import org.apache.http.HttpStatus;
import org.folio.inventory.support.http.ContentType;

public class BadRequestException extends AbstractInventoryException {
  public BadRequestException(String message) {
    super("Bad request:", message, HttpStatus.SC_BAD_REQUEST, ContentType.TEXT_PLAIN);
  }
}
