package org.folio.inventory.storage.external;

import static java.lang.String.format;
import static org.apache.http.HttpHeaders.ACCEPT;

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
import org.folio.inventory.domain.items.CQLQueryRequestDto;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.util.PercentCodec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

  public void add(T item, Consumer<Success<T>> resultCallback,
                  Consumer<Failure> failureCallback) {
    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(storageAddress));
    request
      .sendJsonObject(mapToRequest(item))
      .onSuccess(response -> {
        if (response.statusCode() == 201) {
          try {
            T created = mapFromJson(response.bodyAsJsonObject());
            resultCallback.accept(new Success<>(created));
          } catch (Exception e) {
            LOGGER.error(e);
            failureCallback.accept(new Failure(e.getMessage(), response.statusCode()));
          }
        } else {
          failureCallback.accept(new Failure(response.bodyAsString(), response.statusCode()));
        }
      })
      .onFailure(error -> failureCallback.accept(new Failure(error.getMessage(), 500)));
  }

  public void findById(String id,
                       Consumer<Success<T>> resultCallback,
                       Consumer<Failure> failureCallback) {

    final HttpRequest<Buffer> request = withStandardHeaders(
      webClient.getAbs(individualRecordLocation(id)));

    request.send()
      .onSuccess(response -> {
        switch (response.statusCode()) {
          case 200:
            try {
              JsonObject instanceFromServer = response.bodyAsJsonObject();
              T found = mapFromJson(instanceFromServer);
              resultCallback.accept(new Success<>(found));
            } catch (Exception e) {
              LOGGER.error(e);
              failureCallback.accept(new Failure(e.getMessage(), 500));
            }
            break;

          case 404:
            resultCallback.accept(new Success<>(null));
            break;

          default:
            failureCallback.accept(new Failure(response.bodyAsString(), response.statusCode()));
            break;
        }
      })
      .onFailure(error -> {
        LOGGER.error("Request to find record by id '{}' failed to send", id, error);
        failureCallback.accept(new Failure(error.getMessage(), -1));
      });
  }

  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = format("%s?limit=%s&offset=%s", storageAddress,
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

    String location = format("%s?query=%s&limit=%s&offset=%s",
      storageAddress, encodedQuery, pagingParameters.limit, pagingParameters.offset);
    find(location, resultCallback, failureCallback);
  }

  public void retrieveByCqlBody(CQLQueryRequestDto cqlQueryRequestDto,
                                Consumer<Success<MultipleRecords<T>>> resultCallback,
                                Consumer<Failure> failureCallback) {
    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(storageAddress + "/retrieve"));

    request.sendJsonObject(JsonObject.mapFrom(cqlQueryRequestDto))
      .onSuccess(response -> interpretMultipleRecordResponse(resultCallback, failureCallback, response))
      .onFailure(error -> {
        LOGGER.error("Request to {} failed to send", storageAddress, error);
        failureCallback.accept(new Failure(error.getMessage(), -1));
      });
  }

  public void update(T item,
                     Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(getId(item));
    final HttpRequest<Buffer> request = withStandardHeaders(webClient.putAbs(location));
    request.sendJsonObject(mapToRequest(item))
      .onSuccess(response -> interpretNoContentResponse(response, completionCallback, failureCallback))
      .onFailure(error -> {
        LOGGER.error("Request to update record at {} failed to send", location, error);
        failureCallback.accept(new Failure(error.getMessage(), -1));
      });
  }

    public void delete(String id, Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    deleteLocation(individualRecordLocation(id), completionCallback, failureCallback);
  }

  protected String individualRecordLocation(String id) {
    return format("%s/%s", storageAddress, id);
  }

  protected HttpRequest<Buffer> withStandardHeaders(HttpRequest<Buffer> request) {
    request.putHeader(ACCEPT, "application/json, text/plain")
      .putHeader(TENANT_HEADER, tenant)
      .putHeader(TOKEN_HEADER, token)
      .putHeader(REQUEST_ID_HEADER, requestId);

    if (!userId.isBlank()) {
      request.putHeader(USER_ID_HEADER, userId);
    }
    return request;
  }

  private void find(String location,
                    Consumer<Success<MultipleRecords<T>>> resultCallback,
                    Consumer<Failure> failureCallback) {

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.getAbs(location));
    request.send()
      .onSuccess(response -> interpretMultipleRecordResponse(resultCallback, failureCallback, response))
      .onFailure(error -> {
        LOGGER.error("Request to find records at {} failed to send", location, error);
        failureCallback.accept(new Failure(error.getMessage(), -1));
      });
  }

  private void interpretMultipleRecordResponse(
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback,
    HttpResponse<Buffer> response) {

    if (response.statusCode() == 200) {
      try {
        JsonObject wrappedRecords = response.bodyAsJsonObject();
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
        failureCallback.accept(new Failure(e.getMessage(), response.statusCode()));
      }
    } else {
      failureCallback.accept(new Failure(response.bodyAsString(), response.statusCode()));
    }
  }

  private void deleteLocation(String location,
                              Consumer<Success<Void>> completionCallback,
                              Consumer<Failure> failureCallback) {

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.deleteAbs(location));
    request.send()
      .onSuccess(response -> interpretNoContentResponse(response, completionCallback, failureCallback))
      .onFailure(error -> {
        LOGGER.error("Request to delete record at {} failed to send", location, error);
        failureCallback.accept(new Failure(error.getMessage(), -1));
      });
  }

  private void interpretNoContentResponse(HttpResponse<Buffer> response,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {
    if (response.statusCode() == 204) {
      completionCallback.accept(new Success<>(null));
    } else {
      failureCallback.accept(new Failure(response.bodyAsString(), response.statusCode()));
    }
  }

}
