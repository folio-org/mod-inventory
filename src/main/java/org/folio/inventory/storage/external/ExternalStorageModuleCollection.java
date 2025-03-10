package org.folio.inventory.storage.external;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.Collectors;

abstract class ExternalStorageModuleCollection<T> {
  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String USER_ID_HEADER = "X-Okapi-User-Id";
  private static final String REQUEST_ID_HEADER = "X-Okapi-Request-Id";

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleCollection.class);


  protected final String storageAddress;
  protected final String tenant;
  protected final String token;
  protected final String collectionWrapperPropertyName;
  protected final WebClient webClient;
  protected final String userId;
  protected final String requestId;

  ExternalStorageModuleCollection(
    String storageAddress,
    String tenant,
    String token,
    String userId,
    String requestId,
    String collectionWrapperPropertyName,
    HttpClient client) {

    this.storageAddress = storageAddress;
    this.tenant = tenant;
    this.token = token;
    this.userId = userId;
    this.requestId = requestId;
    this.collectionWrapperPropertyName = collectionWrapperPropertyName;
    this.webClient = WebClient.wrap(client);
  }

  protected abstract JsonObject mapToRequest(T record);

  protected abstract T mapFromJson(JsonObject fromServer);

  protected abstract String getId(T record);

  public void add(T item,
                  Consumer<Success<T>> resultCallback,
                  Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(storageAddress));

    request.sendJsonObject(mapToRequest(item), futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response -> {
        if (response.getStatusCode() == 201) {
          try {
            T created = mapFromJson(response.getJson());
            resultCallback.accept(new Success<>(created));
          } catch (Exception e) {
            LOGGER.error(e);
            failureCallback.accept(new Failure(e.getMessage(), response.getStatusCode()));
          }
        } else {
          failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });

  }

  public void findById(String id,
                       Consumer<Success<T>> resultCallback,
                       Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(
      webClient.getAbs(individualRecordLocation(id)));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response -> {
        switch (response.getStatusCode()) {
          case 200:
            JsonObject instanceFromServer = response.getJson();

            try {
              T found = mapFromJson(instanceFromServer);
              resultCallback.accept(new Success<>(found));
              break;
            } catch (Exception e) {
              LOGGER.error(e);
              failureCallback.accept(new Failure(e.getMessage(), 500));
              break;
            }

          case 404:
            resultCallback.accept(new Success<>(null));
            break;

          default:
            failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });
  }

  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = String.format(storageAddress
        + "?limit=%s&offset=%s",
      pagingParameters.limit, pagingParameters.offset);

    find(location, resultCallback, failureCallback);
  }

  public void empty(
    String cqlQuery,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    if (cqlQuery == null) {
      failureCallback.accept(new Failure("query parameter is required", 400));
      return;
    }
    deleteLocation(storageAddress + "?query=" + PercentCodec.encode(cqlQuery), completionCallback, failureCallback);
  }

  public void findByCql(String cqlQuery,
                        PagingParameters pagingParameters,
                        Consumer<Success<MultipleRecords<T>>> resultCallback,
                        Consumer<Failure> failureCallback) {

    String encodedQuery = URLEncoder.encode(cqlQuery, StandardCharsets.UTF_8);

    String location =
      String.format("%s?query=%s", storageAddress, encodedQuery) +
        String.format("&limit=%s&offset=%s", pagingParameters.limit,
          pagingParameters.offset);

    find(location, resultCallback, failureCallback);
  }

  public void update(T item,
                     Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(getId(item));

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();
    LOGGER.info("update:: location: {}", location);
    LOGGER.info("update:: item: {}", item);
    final HttpRequest<Buffer> request = withStandardHeaders(webClient.putAbs(location));
    LOGGER.info("update:: request.headers(): {}", request.headers());
    LOGGER.info("update:: mapToRequest(item): {}", mapToRequest(item));
    request.sendJsonObject(mapToRequest(item), futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response ->
        interpretNoContentResponse(response, completionCallback, failureCallback));
  }

  public void delete(String id, Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    deleteLocation(individualRecordLocation(id), completionCallback, failureCallback);
  }

  protected String individualRecordLocation(String id) {
    return String.format("%s/%s", storageAddress, id);
  }

  void includeIfPresent(
    JsonObject instanceToSend,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }

  protected HttpRequest<Buffer> withStandardHeaders(HttpRequest<Buffer> request) {
    LOGGER.info("withStandardHeaders:: userId: {}", userId);
    return request
      .putHeader(ACCEPT, "application/json, text/plain")
      .putHeader(TENANT_HEADER, tenant)
      .putHeader(TOKEN_HEADER, token)
      .putHeader(USER_ID_HEADER, userId)
      .putHeader(REQUEST_ID_HEADER, requestId);
  }

  protected CompletionStage<Response> mapAsyncResultToCompletionStage(
    AsyncResult<HttpResponse<Buffer>> asyncResult) {

    return asyncResult.succeeded()
      ? completedFuture(mapResponse(asyncResult))
      : failedFuture(asyncResult.cause());
  }

  private Response mapResponse(AsyncResult<HttpResponse<Buffer>> asyncResult) {
    final var response = asyncResult.result();

    return new Response(response.statusCode(), response.bodyAsString(),
      response.getHeader(CONTENT_TYPE), response.getHeader(LOCATION));
  }

  private void find(String location,
                    Consumer<Success<MultipleRecords<T>>> resultCallback, Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.getAbs(location));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response ->
        interpretMultipleRecordResponse(resultCallback, failureCallback, response));
  }

  private void interpretMultipleRecordResponse(
    Consumer<Success<MultipleRecords<T>>> resultCallback, Consumer<Failure> failureCallback,
    Response response) {

    if (response.getStatusCode() == 200) {
      try {
        JsonObject wrappedRecords = response.getJson();

        List<JsonObject> records = JsonArrayHelper.toList(
          wrappedRecords.getJsonArray(collectionWrapperPropertyName));

        List<T> foundRecords = records.stream()
          .map(this::mapFromJson)
          .collect(Collectors.toList());

        MultipleRecords<T> result = new MultipleRecords<>(
          foundRecords, wrappedRecords.getInteger("totalRecords"));

        resultCallback.accept(new Success<>(result));
      } catch (Exception e) {
        LOGGER.error(e);
        failureCallback.accept(new Failure(e.getMessage(), response.getStatusCode()));
      }

    } else {
      failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
    }
  }

  private void deleteLocation(String location, Consumer<Success<Void>> completionCallback,
                              Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.deleteAbs(location));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response ->
        interpretNoContentResponse(response, completionCallback, failureCallback));
  }

  private void interpretNoContentResponse(Response response, Consumer<Success<Void>> completionCallback, Consumer<Failure> failureCallback) {
    if (response.getStatusCode() == 204) {
      completionCallback.accept(new Success<>(null));
    } else {
      failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
    }
  }
}
