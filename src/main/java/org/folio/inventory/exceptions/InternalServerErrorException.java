package org.folio.inventory.exceptions;

import org.apache.http.HttpStatus;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.support.http.ContentType;

public class InternalServerErrorException extends ExternalResourceFetchException {
  public InternalServerErrorException(Throwable ex) {
    super(ex.getMessage(), HttpStatus.SC_INTERNAL_SERVER_ERROR, ContentType.TEXT_PLAIN);
  }

  public InternalServerErrorException(String reason) {
    super(reason, HttpStatus.SC_INTERNAL_SERVER_ERROR, ContentType.TEXT_PLAIN);
  }
  public InternalServerErrorException(Failure failure){
    super(failure.getReason(), failure.getStatusCode(), ContentType.TEXT_PLAIN);
  }
}
