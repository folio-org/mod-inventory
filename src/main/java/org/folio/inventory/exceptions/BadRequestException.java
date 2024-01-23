package org.folio.inventory.exceptions;

import org.apache.http.HttpStatus;
import org.folio.inventory.support.http.ContentType;

/**
 * Exception for invalid data at request, 400 status code
 */
public class BadRequestException extends AbstractInventoryException {
  public BadRequestException(String message) {
    super("Bad request:", message, HttpStatus.SC_BAD_REQUEST, ContentType.TEXT_PLAIN);
  }
}
