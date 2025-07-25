package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.util.PercentCodec;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.StringJoiner;
import static java.lang.String.format;
import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

/**
 * Wrapper class for SourceStorageRecordsClient to handle POST and PUT HTTP requests with x-okapi-user-id header.
 */
public class SourceStorageRecordsClientWrapper extends SourceStorageRecordsClient {
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
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
    return createRequest(HttpMethod.POST, okapiUrl + "/source-storage/records", okapiUrl, tenantId, token, userId, webClient)
      .sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsById(String id, Record aRecord) {
    return createRequest(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id, okapiUrl, tenantId, token, userId, webClient)
      .sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsGenerationById(String id, Record aRecord) {
    return createRequest(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id + "/generation",
      okapiUrl, tenantId, token, userId, webClient)
      .sendBuffer(getBuffer(aRecord));
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

    return createRequest(HttpMethod.PUT, okapiUrl + SOURCE_STORAGE_RECORDS + id + "/suppress-from-discovery" + queryParams,
      okapiUrl, tenantId, token, userId, webClient)
      .send();
  }

}
