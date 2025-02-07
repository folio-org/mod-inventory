package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.Snapshot;

import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

/**
 * Wrapper class for SourceStorageSnapshotsClient to handle POST and PUT HTTP requests with x-okapi-user-id header.
 */
public class SourceStorageSnapshotsClientWrapper extends SourceStorageSnapshotsClient {
  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final String userId;
  private final WebClient webClient;

  public SourceStorageSnapshotsClientWrapper(String okapiUrl, String tenantId, String token, String userId, HttpClient httpClient) {
    super(okapiUrl, tenantId, token, httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageSnapshots(Snapshot snapshot) {
    return createRequest(HttpMethod.POST, okapiUrl + "/source-storage/snapshots", okapiUrl, tenantId, token, userId, webClient)
      .sendBuffer(getBuffer(snapshot));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, Snapshot snapshot) {
    return createRequest(HttpMethod.PUT, okapiUrl + "/source-storage/snapshots/" + jobExecutionId,
      okapiUrl, tenantId, token, userId, webClient)
      .sendBuffer(getBuffer(snapshot));
  }
}
