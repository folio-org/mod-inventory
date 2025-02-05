package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Record;
import org.folio.util.PercentCodec;

import static org.folio.inventory.client.util.ClientWrapperUtil.ACCEPT;
import static org.folio.inventory.client.util.ClientWrapperUtil.APPLICATION_JSON;
import static org.folio.inventory.client.util.ClientWrapperUtil.APPLICATION_JSON_TEXT_PLAIN;
import static org.folio.inventory.client.util.ClientWrapperUtil.CONTENT_TYPE;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;
import static org.folio.inventory.client.util.ClientWrapperUtil.populateOkapiHeaders;

public class SourceStorageRecordsClientWrapper extends SourceStorageRecordsClient {
  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final String userId;
  private final WebClient webClient;
  public static final String SOURCE_STORAGE_RECORDS = "/source-storage/records/";

  public SourceStorageRecordsClientWrapper(String okapiUrl, String tenantId, String token, String userId, HttpClient httpClient) {
    super(okapiUrl, tenantId, token, httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageRecords(Record aRecord) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, okapiUrl + "/source-storage/records");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);

    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsById(String id, Record aRecord) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id);
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsGenerationById(String id, Record aRecord) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id + "/generation");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(aRecord));
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
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id + "/suppress-from-discovery" + queryParams);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.send();
  }
}
