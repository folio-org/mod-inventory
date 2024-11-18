package org.folio.inventory.support.http.client;

import static org.apache.http.HttpHeaders.ACCEPT;

import java.net.URL;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public abstract class AbstractOkapiHttpClient {

  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";
  private static final String OKAPI_USER_ID_HEADER = "X-Okapi-User-Id";
  private static final String OKAPI_REQUEST_ID = "X-Okapi-Request-Id";

  private final URL okapiUrl;
  private final String tenantId;
  private final String token;
  private final String userId;
  private final String requestId;
  private final Consumer<Throwable> exceptionHandler;

  public Map<String, String> getHeadersMap() {
    return Map.of(
      ACCEPT, "application/json, text/plain",
      OKAPI_URL_HEADER, this.okapiUrl.toString(),
      TENANT_HEADER, this.tenantId,
      TOKEN_HEADER, this.token,
      OKAPI_USER_ID_HEADER, this.userId,
      OKAPI_REQUEST_ID, this.requestId
    );
  }
}
