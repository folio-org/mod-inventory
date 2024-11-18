package org.folio.inventory.support.http.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.inventory.common.WebContext;

public class OkapiHttpClient extends AbstractOkapiHttpClient {

  private final WebClient webClient;

  static Map<Vertx,WebClient> webClients = new HashMap<>();

  static WebClient getWebClient(Vertx vertx) {
    return webClients.computeIfAbsent(vertx, WebClient::create);
  }

  /** HTTP client that calls via Okapi
   *
   * @param vertx Vert.x handle
   * @param okapiUrl Okapi URL (java.net.URL)
   * @param tenantId Okapi tenantId - ignored if blank/empty
   * @param token - Okapi token - ignored if blank/empty
   * @param userId - Folio User ID - ignored if blank/empty
   * @param requestId - Okapi Request ID - ignored if null
   * @param exceptionHandler - exceptionHandler (for POST only, not PUT??)
   */
  public OkapiHttpClient(Vertx vertx, URL okapiUrl, String tenantId,
    String token, String userId, String requestId, Consumer<Throwable> exceptionHandler) {
    this(getWebClient(vertx), okapiUrl, tenantId, token, userId, requestId, exceptionHandler);
  }

  public OkapiHttpClient(WebClient webClient, WebContext context,
    Consumer<Throwable> exceptionHandler) throws MalformedURLException {

    this(webClient, new URL(context.getOkapiLocation()),
      context.getTenantId(), context.getToken(), context.getUserId(),
      context.getRequestId(), exceptionHandler);
  }

  /** HTTP client that calls via Okapi
   *
   * @param webClient web client to use for HTTP requests
   * @param okapiUrl Okapi URL (java.net.URL)
   * @param tenantId Okapi tenantId - ignored if blank/empty
   * @param token - Okapi token - ignored if blank/empty
   * @param userId - Folio User ID - ignored if blank/empty
   * @param requestId - Okapi Request ID - ignored if null
   * @param exceptionHandler - exceptionHandler (for POST only, not PUT??)
   */
  public OkapiHttpClient(WebClient webClient, URL okapiUrl, String tenantId,
    String token, String userId, String requestId, Consumer<Throwable> exceptionHandler) {

    super(okapiUrl, tenantId, userId, token, requestId, exceptionHandler);
    this.webClient = webClient;
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

  public CompletionStage<Response> post(String url, String body, Map<String, String> headers) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(url));

    for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
      request.putHeader(headerEntry.getKey(), headerEntry.getValue());
    }

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

  public CompletionStage<Response> get(URL url) {
    return get(url.toString());
  }

  public CompletionStage<Response> get(String url) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.getAbs(url));

    request.send(futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public CompletionStage<Response> get(String url, Map<String, String> params) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.getAbs(url));
    params.forEach(request::addQueryParam);

    request.send(futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  public CompletionStage<Response> delete(URL url) {
    return delete(url.toString());
  }

  public CompletionStage<Response> delete(String url) {
    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.deleteAbs(url));

    request.send(futureResponse::complete);

    return futureResponse
      .thenCompose(OkapiHttpClient::mapAsyncResultToCompletionStage);
  }

  private HttpRequest<Buffer> withStandardHeaders(HttpRequest<Buffer> request) {
    getHeadersMap().forEach(request::putHeader);
    return request;
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
