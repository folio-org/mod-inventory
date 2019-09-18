package org.folio.inventory.support.http.client;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.http.ContentType;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Consumer;

public class OkapiHttpClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";
  private static final String OKAPI_USER_ID_HEADER = "X-Okapi-User-Id";
  private static final String OKAPI_REQUEST_ID = "X-Okapi-Request-Id";

  private final HttpClient client;
  private final URL okapiUrl;
  private final String tenantId;
  private final String token;
  private final String userId;
  private final String requestId;
  private final Consumer<Throwable> exceptionHandler;

  public OkapiHttpClient(HttpClient httpClient,
    WebContext context, Consumer<Throwable> exceptionHandler)
    throws MalformedURLException {

    this(httpClient, new URL(context.getOkapiLocation()),
      context.getTenantId(), context.getToken(), context.getUserId(),
      context.getRequestId(), exceptionHandler);
  }

  /** HTTP client that calls via Okapi
   *
   * @param httpClient as returned from vertx createHttpClient
   * @param okapiUrl OkapiUrl string (must not be null)
   * @param tenantId Okapi tenantId - ignored if blank/empty
   * @param token - Okapi token - ignored if blank/empty
   * @param userId - Folio User ID - ignored if blank/empty
   * @param requestId - Okapi Request ID - ignored if null
   * @param exceptionHandler - exceptionHandler (for POST only, not PUT??)
   */
  public OkapiHttpClient(HttpClient httpClient,
    URL okapiUrl,
    String tenantId,
    String token,
    String userId,
    String requestId,
    Consumer<Throwable> exceptionHandler) {

    this.client = httpClient;
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.userId = userId;
    this.token = token;
    this.requestId = requestId;
    this.exceptionHandler = exceptionHandler;
  }

  public void post(URL url,
                   Object body,
                   Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.postAbs(url.toString(), responseHandler);

    okapiHeaders(request);
    accept(request, ContentType.APPLICATION_JSON, ContentType.TEXT_PLAIN);
    jsonContentType(request);

    request.setTimeout(5000);

    request.exceptionHandler(this.exceptionHandler::accept);

    if(body != null) {
      String encodedBody = Json.encodePrettily(body);

      log.info(String.format("POST %s, Request: %s",
        url.toString(), encodedBody));

      request.end(encodedBody);
    }
    else {
      request.end();
    }
  }

  public void put(URL url,
                  Object body,
                  Handler<HttpClientResponse> responseHandler) {

    put(url.toString(), body, responseHandler);
  }

  public void put(String url,
                  Object body,
                  Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.putAbs(url, responseHandler);

    okapiHeaders(request);
    accept(request, ContentType.APPLICATION_JSON, ContentType.TEXT_PLAIN);
    jsonContentType(request);

    String encodedBody = Json.encodePrettily(body);

    log.info(String.format("PUT %s, Request: %s", url, encodedBody));

    request.end(encodedBody);
  }

  public void get(URL url, Handler<HttpClientResponse> responseHandler) {

    get(url.toString(), responseHandler);
  }

  public void get(URL url,
                  String query,
                  Handler<HttpClientResponse> responseHandler)
    throws MalformedURLException {

    get(new URL(url.getProtocol(), url.getHost(), url.getPort(),
        url.getPath() + "?" + query),
      responseHandler);
  }

  public void get(String url, Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.getAbs(url, responseHandler);

    accept(request, ContentType.APPLICATION_JSON);

    okapiHeaders(request);

    log.info(String.format("GET %s", url));

    request.end();
  }

  public void delete(URL url, Handler<HttpClientResponse> responseHandler) {

    delete(url.toString(), responseHandler);
  }

  public void delete(String url, Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.deleteAbs(url, responseHandler);

    accept(request, ContentType.APPLICATION_JSON, ContentType.TEXT_PLAIN);

    okapiHeaders(request);

    request.end();
  }

  private void okapiHeaders(HttpClientRequest request) {
    if(StringUtils.isNotBlank(this.tenantId)) {
      request.headers().add(TENANT_HEADER, this.tenantId);
    }

    if(StringUtils.isNotBlank(this.token)) {
      request.headers().add(TOKEN_HEADER, this.token);
    }

    if (this.requestId != null) {
      request.headers().add(OKAPI_REQUEST_ID, this.requestId);
    }

    request.headers().add(OKAPI_URL_HEADER, okapiUrl.toString());
    request.headers().add(OKAPI_USER_ID_HEADER, userId);
  }

  private static void accept(
    HttpClientRequest request,
    String... contentTypes) {

    request.putHeader(HttpHeaders.ACCEPT, StringUtils.join(contentTypes, ","));
  }

  private void jsonContentType(HttpClientRequest request) {
    request.putHeader(HttpHeaders.CONTENT_TYPE.toString(),
      ContentType.APPLICATION_JSON);
  }
}
