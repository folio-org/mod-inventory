package org.folio.inventory.client;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.ClientHelpers;
import org.folio.util.PercentCodec;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;

public class SourceStorageRecordsClientWrapper extends SourceStorageRecordsClient {
  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final String userId;
  private final WebClient webClient;

  public SourceStorageRecordsClientWrapper(String okapiUrl, String tenantId, String token, String userId, HttpClient httpClient) {
    super(tenantId, token, okapiUrl, httpClient);
    this.tenantId = tenantId;
    this.token = token;
    this.okapiUrl = okapiUrl;
    this.userId = userId;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageRecords(Record Record) {
    Buffer buffer = Buffer.buffer();
    if (Record != null) {
      buffer.appendString(ClientHelpers.pojo2json(Record));
    }

    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.POST, okapiUrl + "/source-storage/records");
    request.putHeader("Content-type", "application/json");
    request.putHeader("Accept", "application/json,text/plain");

    populateOkapiHeaders(request);

    return request.sendBuffer(buffer);
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsById(String id, Record Record) {
    Buffer buffer = Buffer.buffer();
    if (Record != null) {
      buffer.appendString(ClientHelpers.pojo2json(Record));
    }

    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.PUT, this.okapiUrl + "/source-storage/records/" + id);
    request.putHeader("Content-type", "application/json");
    request.putHeader("Accept", "application/json,text/plain");
    populateOkapiHeaders(request);

    return request.sendBuffer(buffer);
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsGenerationById(String id, Record Record) {
    Buffer buffer = Buffer.buffer();
    if (Record != null) {
      buffer.appendString(ClientHelpers.pojo2json(Record));
    }

    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.PUT, this.okapiUrl + "/source-storage/records/" + id + "/generation");
    request.putHeader("Content-type", "application/json");
    request.putHeader("Accept", "application/json");
    populateOkapiHeaders(request);

    return request.sendBuffer(buffer);
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsSuppressFromDiscoveryById(String id, String idType, boolean suppress) {
    StringBuilder queryParams = new StringBuilder("?");
    if (idType != null) {
      queryParams.append("idType=");
      queryParams.append(PercentCodec.encode(idType));
      queryParams.append("&");
    }

    queryParams.append("suppress=");
    queryParams.append(suppress);
    HttpRequest<Buffer> request = this.webClient.requestAbs(HttpMethod.PUT, this.okapiUrl + "/source-storage/records/" + id + "/suppress-from-discovery" + queryParams);
    request.putHeader("Accept", "application/json,text/plain");
    populateOkapiHeaders(request);

    return request.send();
  }

  private void populateOkapiHeaders(HttpRequest<Buffer> request) {
    if (this.tenantId != null) {
      request.putHeader(OKAPI_TOKEN, this.token);
      request.putHeader(OKAPI_TENANT, this.tenantId);
    }

    if (this.userId != null) {
      request.putHeader(OKAPI_USER_ID, this.userId);
    }

    if (this.okapiUrl != null) {
      request.putHeader(OKAPI_URL, this.okapiUrl);
    }
  }
}
