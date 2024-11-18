package org.folio.inventory.support.http.client;

import static org.apache.http.HttpHeaders.ACCEPT;

import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;

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
  private final Map<String, String> headers;

  protected AbstractOkapiHttpClient(URL okapiUrl, String tenantId, String token, String userId, String requestId, Consumer<Throwable> exceptionHandler) {
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.requestId = requestId;
    this.exceptionHandler = exceptionHandler;
    this.headers = createHeadersMap();
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  private Map<String, String> createHeadersMap() {
    return Stream.of(
        Map.entry(ACCEPT, Optional.of("application/json, text/plain")),
        Map.entry(OKAPI_URL_HEADER, Optional.ofNullable(this.okapiUrl).map(URL::toString)),
        Map.entry(TENANT_HEADER, Optional.ofNullable(this.tenantId)),
        Map.entry(TOKEN_HEADER, Optional.ofNullable(this.token)),
        Map.entry(OKAPI_USER_ID_HEADER, Optional.ofNullable(this.userId)),
        Map.entry(OKAPI_REQUEST_ID, Optional.ofNullable(this.requestId))
      )
      .filter(entry -> entry.getValue().isPresent())
      .map(entry -> Map.entry(entry.getKey(), entry.getValue().get()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
