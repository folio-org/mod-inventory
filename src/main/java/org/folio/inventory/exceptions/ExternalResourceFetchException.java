package org.folio.inventory.exceptions;

import org.folio.inventory.support.http.client.Response;

public class ExternalResourceFetchException extends AbstractInventoryException {
  private final String body;
  private final int statusCode;
  private final String contentType;

  public ExternalResourceFetchException(String body, int statusCode, String contentType) {
    super("External resource fetch exception: " + body);
    this.body = body;
    this.statusCode = statusCode;
    this.contentType = contentType;
  }

  public ExternalResourceFetchException(Response response) {
    this(response.getBody(), response.getStatusCode(), response.getContentType());
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
