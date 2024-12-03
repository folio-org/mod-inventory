package org.folio.inventory.exceptions;

import org.folio.inventory.support.http.client.Response;

public class ExternalResourceFetchException extends AbstractInventoryException {

  public ExternalResourceFetchException(String message, String body, int statusCode, String contentType) {
    super(message, body, statusCode, contentType);
  }

  public ExternalResourceFetchException(String body, int statusCode, String contentType) {
    super("External resource fetch exception:", body, statusCode, contentType);
  }

  public ExternalResourceFetchException(Response response) {
    this(response.getBody(), response.getStatusCode(), response.getContentType());
  }
}
