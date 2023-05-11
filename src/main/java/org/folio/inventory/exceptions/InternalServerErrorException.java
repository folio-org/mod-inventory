package org.folio.inventory.exceptions;

import org.apache.http.HttpStatus;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.support.http.ContentType;

public class InternalServerErrorException extends AbstractInventoryException {
  public InternalServerErrorException(Throwable ex) {
    this(ex.getMessage());
  }

  public InternalServerErrorException(String reason) {
    super("Internal server exception:", reason, HttpStatus.SC_INTERNAL_SERVER_ERROR, ContentType.TEXT_PLAIN);
  }
  public InternalServerErrorException(Failure failure){
    super("Internal server exception:", failure.getReason(), failure.getStatusCode(), ContentType.TEXT_PLAIN);
  }
}
