package org.folio.inventory.support.http.client;

import io.netty.handler.codec.http.HttpHeaderValues;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.folio.HttpHeaders;
import org.folio.okapi.common.XOkapiHeaders;

@Getter
public abstract class AbstractOkapiHttpClient {

  private final URL okapiUrl;
  private final String tenantId;
  private final String token;
  private final String userId;
  private final String requestId;
  private final Consumer<Throwable> exceptionHandler;
  private final Map<String, String> headers;

  protected AbstractOkapiHttpClient(URL okapiUrl, String tenantId, String userId, String token, String requestId,
                                    Consumer<Throwable> exceptionHandler) {
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.requestId = requestId;
    this.exceptionHandler = exceptionHandler;
    this.headers = createHeadersMap();
  }

  private Map<String, String> createHeadersMap() {
    return Stream.of(
        Map.entry(HttpHeaders.ACCEPT, Optional.of(HttpHeaderValues.APPLICATION_JSON.concat(",")
          .concat(HttpHeaderValues.TEXT_PLAIN).toString())),
        Map.entry(XOkapiHeaders.URL, Optional.ofNullable(this.okapiUrl).map(URL::toString)),
        Map.entry(XOkapiHeaders.TENANT, Optional.ofNullable(this.tenantId)),
        Map.entry(XOkapiHeaders.TOKEN, Optional.ofNullable(this.token)),
        Map.entry(XOkapiHeaders.USER_ID, Optional.ofNullable(this.userId)),
        Map.entry(XOkapiHeaders.REQUEST_ID, Optional.ofNullable(this.requestId))
      )
      .filter(entry -> entry.getValue().isPresent())
      .map(entry -> Map.entry(entry.getKey(), entry.getValue().get()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
