package org.folio.inventory;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Record;

import static org.folio.inventory.Launcher.systemUserEnabled;

public class SourceStorageRecordsClientWrapper extends SourceStorageRecordsClient {

  protected static final Logger LOGGER = LogManager.getLogger(SourceStorageRecordsClientWrapper.class);

  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final WebClient webClient;

  private static String getToken(String token) {
    return systemUserEnabled ? token : null;
  }

  public SourceStorageRecordsClientWrapper(String okapiUrl, String tenantId, String token, HttpClient httpClient) {
    super(okapiUrl, tenantId, getToken(token), httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = getToken(token);
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postSourceStorageRecords(Record Record) {
    StringBuilder queryParams = new StringBuilder("?");
    Buffer buffer = Buffer.buffer();
    if (Record!= null) {
      buffer.appendString(org.folio.rest.tools.ClientHelpers.pojo2json(Record));
    }

    io.vertx.ext.web.client.HttpRequest<Buffer> request =
      webClient.requestAbs(io.vertx.core.http.HttpMethod.POST,
        okapiUrl + "/source-storage/records" + queryParams.toString());

    request.putHeader("Content-type", "application/json");
    request.putHeader("Accept", "application/json,text/plain");

    if (tenantId != null) {
      if (!StringUtils.isEmpty(token)) {
        LOGGER.info("Request by system user.");
        request.putHeader("X-Okapi-Token", token);
      } else {
        LOGGER.info("Request supported by sidecar.");
      }
      request.putHeader("x-okapi-tenant", tenantId);
    }

    if (okapiUrl != null) {
      request.putHeader("X-Okapi-Url", okapiUrl);
    }

    return request.sendBuffer(buffer);
  }

}
