package org.folio.inventory.support.http.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.folio.inventory.common.WebContext;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class OkapiHttpClient {

  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";
  private static final String OKAPI_USER_ID_HEADER = "X-Okapi-User-Id";
  private static final String OKAPI_REQUEST_ID = "X-Okapi-Request-Id";

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
