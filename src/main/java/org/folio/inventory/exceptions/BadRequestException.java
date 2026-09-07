package org.folio.inventory.exceptions;

import io.netty.handler.codec.http.HttpHeaderValues;
import org.folio.HttpStatus;

/**
 * Exception for invalid data at request, 400 status code.
 */
public class BadRequestException extends AbstractInventoryException {
  public BadRequestException(String message) {
    super("Bad request:", message, HttpStatus.SC_BAD_REQUEST, HttpHeaderValues.TEXT_PLAIN.toString());
  }
}
