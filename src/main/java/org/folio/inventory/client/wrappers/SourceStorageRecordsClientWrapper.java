package org.folio.inventory.client.wrappers;

import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;
import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Record;
import org.folio.util.PercentCodec;

/**
 * Wrapper class for SourceStorageRecordsClient to handle POST and PUT HTTP requests with x-okapi-user-id header.
 */
public class SourceStorageRecordsClientWrapper extends SourceStorageRecordsClient {

  private static final String RECORDS_PATH = "/source-storage/records";
  private static final String RECORD_BY_ID_PATH = RECORDS_PATH + "/%s";
  private static final String RECORD_GENERATION_PATH = RECORD_BY_ID_PATH + "/generation";
  private static final String RECORD_SUPPRESSION_PATH = RECORD_BY_ID_PATH + "/suppress-from-discovery";

  private final WebClient webClient;
  private final FolioHeaders folioHeaders;

  public SourceStorageRecordsClientWrapper(FolioHeaders folioHeaders, HttpClient httpClient) {
    super(folioHeaders.getConnectionUrl().orElse(null),
      folioHeaders.getTenantId().orElse(null),
      folioHeaders.getToken().orElse(null),
      httpClient);
    this.folioHeaders = folioHeaders;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageRecords(Record aRecord) {
    return createRequest(POST, RECORDS_PATH, folioHeaders, webClient)
      .sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsById(String id, Record aRecord) {
    return createRequest(PUT, RECORD_BY_ID_PATH.formatted(id), folioHeaders, webClient)
      .sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsGenerationById(String id, Record aRecord) {
    return createRequest(PUT, RECORD_GENERATION_PATH.formatted(id), folioHeaders, webClient)
      .sendBuffer(getBuffer(aRecord));
  }

  @Override
  public Future<HttpResponse<Buffer>> putSourceStorageRecordsSuppressFromDiscoveryById(String id, String idType,
                                                                                       boolean suppress) {
    StringBuilder queryParams = new StringBuilder("?");
    if (idType != null) {
      queryParams.append("idType=");
      queryParams.append(PercentCodec.encode(idType));
      queryParams.append("&");
    }

    queryParams.append("suppress=");
    queryParams.append(suppress);

    return createRequest(PUT, RECORD_SUPPRESSION_PATH.formatted(id) + queryParams, folioHeaders, webClient)
      .send();
  }
}
