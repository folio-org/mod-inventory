package org.folio.inventory.exceptions;

import org.apache.http.HttpStatus;
import org.folio.inventory.support.http.ContentType;

public abstract class AbstractInventoryException extends RuntimeException {
  protected final String body;
  protected final int statusCode;
  protected final String contentType;

  public AbstractInventoryException(String message) {

    super(message);
    this.body = message;
    this.statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    this.contentType = ContentType.TEXT_PLAIN;
  }

  protected AbstractInventoryException(String message, String body, int statusCode, String contentType) {
    super(message + body);

    this.body = body;
    this.statusCode = statusCode;
    this.contentType = contentType;
  }

  public String getBody() {
    return body;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getContentType() {
    return contentType;
  }
}
