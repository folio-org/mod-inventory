package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.Snapshot;

import static org.folio.inventory.client.wrappers.ClientWrapperUtil.ACCEPT;
import static org.folio.inventory.client.wrappers.ClientWrapperUtil.APPLICATION_JSON;
import static org.folio.inventory.client.wrappers.ClientWrapperUtil.APPLICATION_JSON_TEXT_PLAIN;
import static org.folio.inventory.client.wrappers.ClientWrapperUtil.CONTENT_TYPE;
import static org.folio.inventory.client.wrappers.ClientWrapperUtil.getBuffer;
import static org.folio.inventory.client.wrappers.ClientWrapperUtil.populateOkapiHeaders;

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
    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.POST, this.okapiUrl + "/source-storage/snapshots");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(snapshot));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, Snapshot snapshot) {
    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.PUT, this.okapiUrl + "/source-storage/snapshots/" + jobExecutionId);
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(snapshot));
  }
}
