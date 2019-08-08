package org.folio.inventory.storage.external;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.ContentType;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

abstract class ExternalStorageModuleCollection<T> {
  private final Vertx vertx;
  private final String storageAddress;
  private final String tenant;
  private final String token;
  private final String collectionWrapperPropertyName;
  private final HttpClient client;

  ExternalStorageModuleCollection(
    Vertx vertx,
    String storageAddress,
    String tenant,
    String token,
    String collectionWrapperPropertyName,
    HttpClient client) {

    this.vertx = vertx;
    this.storageAddress = storageAddress;
    this.tenant = tenant;
    this.token = token;
    this.collectionWrapperPropertyName = collectionWrapperPropertyName;
    this.client = client;
  }

  protected abstract JsonObject mapToRequest(T record);
  protected abstract T mapFromJson(JsonObject fromServer);
  protected abstract String getId(T record);

  public void add(T item,
    Consumer<Success<T>> resultCallback,
    Consumer<Failure> failureCallback) {

    Handler<HttpClientResponse> onResponse = response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 201) {
          T created = mapFromJson(new JsonObject(responseBody));

          resultCallback.accept(new Success<>(created));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
    });

    JsonObject toSend = mapToRequest(item);

    HttpClientRequest request = createRequest(HttpMethod.POST, storageAddress,
      onResponse, failureCallback);

    jsonContentType(request);
    acceptJson(request);

    request.end(Json.encodePrettily(toSend));
  }

  public void findById(String id,
    Consumer<Success<T>> resultCallback,
    Consumer<Failure> failureCallback) {

    Handler<HttpClientResponse> onResponse =
      response -> response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        switch (statusCode) {
          case 200:
            JsonObject instanceFromServer = new JsonObject(responseBody);

            T found = mapFromJson(instanceFromServer);

            resultCallback.accept(new Success<>(found));
            break;

          case 404:
            resultCallback.accept(new Success<>(null));
            break;

          default:
            failureCallback.accept(new Failure(responseBody, statusCode));
        }
    });

    HttpClientRequest request = createRequest(HttpMethod.GET,
      individualRecordLocation(id), onResponse, failureCallback);

    acceptJson(request);
    request.end();
  }

  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = String.format(storageAddress
        + "?limit=%s&offset=%s",
      pagingParameters.limit, pagingParameters.offset);

    HttpClientRequest request = createRequest(HttpMethod.GET, location,
      handleMultipleResults(resultCallback, failureCallback), failureCallback);

    acceptJson(request);
    request.end();
  }

  public void empty(
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    HttpClientRequest request =
      createRequest(HttpMethod.DELETE, storageAddress, onResponse, failureCallback);

    acceptJsonOrPlainText(request);
    request.end();
  }

  public void findByCql(String cqlQuery,
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) throws UnsupportedEncodingException {

    String encodedQuery = URLEncoder.encode(cqlQuery, "UTF-8");

    String location =
      String.format("%s?query=%s", storageAddress, encodedQuery) +
        String.format("&limit=%s&offset=%s", pagingParameters.limit,
          pagingParameters.offset);

    HttpClientRequest request = createRequest(HttpMethod.GET, location,
      handleMultipleResults(resultCallback, failureCallback), failureCallback);

    acceptJson(request);
    request.end();
  }

  public void update(T item,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(getId(item));

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    JsonObject toSend = mapToRequest(item);

    HttpClientRequest request = createRequest(HttpMethod.PUT, location,
      onResponse, failureCallback);

    jsonContentType(request);
    acceptPlainText(request);

    request.end(Json.encodePrettily(toSend));
  }

  public void delete(String id,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {
    String location = individualRecordLocation(id);

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    HttpClientRequest request = createRequest(HttpMethod.DELETE, location,
      onResponse, failureCallback);

    acceptJsonOrPlainText(request);
    request.end();
  }

  protected void acceptJson(HttpClientRequest request) {
    accept(request, ContentType.APPLICATION_JSON);
  }

  private static void acceptJsonOrPlainText(HttpClientRequest request) {
    accept(request, ContentType.APPLICATION_JSON, ContentType.TEXT_PLAIN);
  }

  private static void acceptPlainText(HttpClientRequest request) {
    accept(request, ContentType.TEXT_PLAIN);
  }

  private static void accept(
    HttpClientRequest request,
    String... contentTypes) {

    request.putHeader(HttpHeaders.ACCEPT, StringUtils.join(contentTypes, ","));
  }

  private Handler<Throwable> exceptionHandler(
    Consumer<Failure> failureCallback) {

    return it -> failureCallback.accept(new Failure(it.getMessage(), null));
  }

  protected void jsonContentType(HttpClientRequest request) {
    request.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON);
  }

  private void addOkapiHeaders(HttpClientRequest request) {
    request.putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token);
  }

  private void registerExceptionHandler(
    HttpClientRequest request,
    Consumer<Failure> failureCallback) {

    request.exceptionHandler(exceptionHandler(failureCallback));
  }

  private Handler<HttpClientResponse> noContentResponseHandler(
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    return response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 204) {
          completionCallback.accept(new Success<>(null));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
  }

  protected HttpClientRequest createRequest(
    HttpMethod method,
    String location,
    Handler<HttpClientResponse> onResponse,
    Consumer<Failure> failureCallback) {

    HttpClientRequest request = client
      .requestAbs(method, location, onResponse);

    registerExceptionHandler(request, failureCallback);
    addOkapiHeaders(request);

    return request;
  }

  private String individualRecordLocation(String id) {
    return String.format("%s/%s", storageAddress, id);
  }

  private Handler<HttpClientResponse> handleMultipleResults(
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    return response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 200) {
          JsonObject wrappedRecords = new JsonObject(responseBody);

          List<JsonObject> records = JsonArrayHelper.toList(
            wrappedRecords.getJsonArray(collectionWrapperPropertyName));

          List<T> foundRecords = records.stream()
            .map(this::mapFromJson)
            .collect(Collectors.toList());

          MultipleRecords<T> result = new MultipleRecords<>(
            foundRecords, wrappedRecords.getInteger("totalRecords"));

          resultCallback.accept(new Success<>(result));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
  }

  void includeIfPresent(
    JsonObject instanceToSend,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }
}
