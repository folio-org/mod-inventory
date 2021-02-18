package org.folio.inventory.support.http.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.http.ContentType;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public class OkapiHttpClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";
  private static final String OKAPI_USER_ID_HEADER = "X-Okapi-User-Id";
  private static final String OKAPI_REQUEST_ID = "X-Okapi-Request-Id";

  private final HttpClient client;
  private final WebClient webClient;
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
   * @param okapiUrl Okapi URL (java.net.URL)
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
    this.webClient = WebClient.wrap(httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.userId = userId;
    this.token = token;
    this.requestId = requestId;
    this.exceptionHandler = exceptionHandler;
  }

  public CompletionStage<Response> post(URL url, JsonObject body) {
    return post(url.toString(), body);
  }

  public CompletionStage<Response> post(String url, JsonObject body) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(url));

    request.sendJsonObject(body, futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public CompletionStage<Response> post(URL url, String body) {
    return post(url.toString(), body);
  }

  public CompletionStage<Response> post(String url, String body) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(url));

    final var buffer = body != null
      ? Buffer.buffer(body)
      : Buffer.buffer();

    request.sendBuffer(buffer, futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public CompletionStage<Response> put(URL url, JsonObject body) {
    return put(url.toString(), body);
  }

  public CompletionStage<Response> put(String url, JsonObject body) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.putAbs(url));

    request.sendJsonObject(body, futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public CompletionStage<Response> get(String url) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.getAbs(url));

    request.send(futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public void get(URL url, Handler<HttpClientResponse> responseHandler) {

    get(url.toString(), responseHandler);
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

    if (this.userId != null) {
      request.headers().add(OKAPI_USER_ID_HEADER, userId);
    }
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

  private HttpRequest<Buffer> withStandardHeaders(HttpRequest<Buffer> request) {
    return request
      .putHeader(ACCEPT, "application/json, text/plain")
      .putHeader(OKAPI_URL_HEADER, okapiUrl.toString())
      .putHeader(TENANT_HEADER, this.tenantId)
      .putHeader(TOKEN_HEADER, this.token)
      .putHeader(OKAPI_USER_ID_HEADER, this.userId)
      .putHeader(OKAPI_REQUEST_ID, this.requestId);
  }

  private static CompletionStage<Response> mapAsyncResultToCompletionStage(
    AsyncResult<HttpResponse<Buffer>> asyncResult) {

    return asyncResult.succeeded()
      ? completedFuture(mapResponse(asyncResult))
      : failedFuture(asyncResult.cause());
  }

  private static Response mapResponse(AsyncResult<HttpResponse<Buffer>> asyncResult) {
    final var response = asyncResult.result();

    return new Response(response.statusCode(), response.bodyAsString(),
      response.getHeader(CONTENT_TYPE), response.getHeader(LOCATION));
  }
}
