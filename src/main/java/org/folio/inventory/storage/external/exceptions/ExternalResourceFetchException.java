package org.folio.inventory.storage.external.exceptions;

import org.folio.inventory.support.http.client.Response;

public class ExternalResourceFetchException extends RuntimeException {
  private final Response failedResponse;

  public ExternalResourceFetchException(Response failedResponse) {
    super("External resource fetch exception" + failedResponse.getBody());
    this.failedResponse = failedResponse;
  }

  public Response getFailedResponse() {
    return failedResponse;
  }
}
