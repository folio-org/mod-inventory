package org.folio.inventory.exceptions;

import io.netty.handler.codec.http.HttpHeaderValues;
import org.folio.HttpStatus;
import org.folio.inventory.common.domain.Failure;

public class InternalServerErrorException extends AbstractInventoryException {
  public InternalServerErrorException(Throwable ex) {
    this(ex.getMessage());
  }

  public InternalServerErrorException(String reason) {
    super("Internal server exception:", reason, HttpStatus.SC_INTERNAL_SERVER_ERROR,
      HttpHeaderValues.TEXT_PLAIN.toString());
  }

  public InternalServerErrorException(Failure failure) {
    super("Internal server exception:", failure.reason(), failure.statusCode(), HttpHeaderValues.TEXT_PLAIN.toString());
  }
}
