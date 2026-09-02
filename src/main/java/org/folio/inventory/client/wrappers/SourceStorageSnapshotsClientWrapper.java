package org.folio.inventory.client.wrappers;

import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.Snapshot;

/**
 * Wrapper class for SourceStorageSnapshotsClient to handle POST and PUT HTTP requests with x-okapi-user-id and x-okapi-request-id headers.
 */
public class SourceStorageSnapshotsClientWrapper extends SourceStorageSnapshotsClient {
  private final WebClient webClient;
  private final FolioHeaders folioHeaders;

  public SourceStorageSnapshotsClientWrapper(FolioHeaders folioHeaders, HttpClient httpClient) {
    super(folioHeaders.getConnectionUrl().orElse(null),
      folioHeaders.getTenantId().orElse(null),
      folioHeaders.getToken().orElse(null),
      httpClient);
    this.folioHeaders = folioHeaders;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageSnapshots(Snapshot snapshot) {
    return createRequest(HttpMethod.POST, "/source-storage/snapshots", folioHeaders, webClient)
      .sendBuffer(getBuffer(snapshot));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageSnapshotsByJobExecutionId(String jobExecutionId,
                                                                                Snapshot snapshot) {
    return createRequest(HttpMethod.PUT, "/source-storage/snapshots/" + jobExecutionId, folioHeaders, webClient)
      .sendBuffer(getBuffer(snapshot));
  }
}
