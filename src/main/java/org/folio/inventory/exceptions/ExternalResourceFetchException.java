package org.folio.inventory.exceptions;

import org.folio.inventory.support.http.client.Response;

public class ExternalResourceFetchException extends AbstractInventoryException {
  private final Response failedResponse;

  public ExternalResourceFetchException(Response failedResponse) {
    super("External resource fetch exception" + failedResponse.getBody());
    this.failedResponse = failedResponse;
  }

  public Response getFailedResponse() {
    return failedResponse;
  }
}
