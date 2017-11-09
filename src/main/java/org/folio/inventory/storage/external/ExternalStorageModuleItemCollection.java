package org.folio.inventory.storage.external;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.Item;
import org.folio.inventory.domain.ItemCollection;
import org.folio.inventory.support.JsonArrayHelper;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class ExternalStorageModuleItemCollection
  implements ItemCollection {

  private final Vertx vertx;
  private final String storageAddress;
  private final String tenant;
  private final String token;

  public ExternalStorageModuleItemCollection(Vertx vertx,
                                          String storageAddress,
                                          String tenant,
                                          String token) {
    this.vertx = vertx;
    this.storageAddress = storageAddress;
    this.tenant = tenant;
    this.token = token;
  }

  @Override
  public void add(Item item,
           Consumer<Success<Item>> resultCallback,
           Consumer<Failure> failureCallback) {

    String location = storageAddress + "/item-storage/items";

    Handler<HttpClientResponse> onResponse = response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 201) {
          Item createdItem = mapFromJson(new JsonObject(responseBody));

          resultCallback.accept(new Success<>(createdItem));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
    });

    JsonObject itemToSend = mapToItemRequest(item);

    vertx.createHttpClient().requestAbs(HttpMethod.POST, location, onResponse)
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "application/json")
      .end(Json.encodePrettily(itemToSend));
  }

  @Override
  public void findById(String id,
                Consumer<Success<Item>> resultCallback,
                Consumer<Failure> failureCallback) {

    String location = String.format("%s/item-storage/items/%s", storageAddress, id);

    Handler<HttpClientResponse> onResponse = response -> {
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        switch (statusCode) {
          case 200:
            JsonObject itemFromServer = new JsonObject(responseBody);

            Item foundItem = mapFromJson(itemFromServer);

            resultCallback.accept(new Success<>(foundItem));
            break;

          case 404:
            resultCallback.accept(new Success<>(null));
            break;

          default:
            failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
    };

    vertx.createHttpClient().requestAbs(HttpMethod.GET, location, onResponse)
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant",  tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Accept", "application/json")
      .end();
  }

  @Override
  public void findAll(PagingParameters pagingParameters,
               Consumer<Success<MultipleRecords<Item>>> resultCallback,
               Consumer<Failure> failureCallback) {

    String location = String.format(storageAddress
      + "/item-storage/items?limit=%s&offset=%s",
      pagingParameters.limit, pagingParameters.offset);

    vertx.createHttpClient().requestAbs(HttpMethod.GET, location,
      handleMultipleResults(resultCallback, failureCallback))
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Accept", "application/json")
      .end();
  }

  @Override
  public void empty(Consumer<Success<Void>> completionCallback,
             Consumer<Failure> failureCallback) {
    String location = storageAddress + "/item-storage/items";

    Handler<HttpClientResponse> onResponse = response ->
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

    vertx.createHttpClient().requestAbs(HttpMethod.DELETE, location, onResponse)
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Accept", "application/json, text/plain")
      .end();
  }

  @Override
  public void findByCql(String cqlQuery,
                 PagingParameters pagingParameters,
                 Consumer<Success<MultipleRecords<Item>>> resultCallback,
                 Consumer<Failure> failureCallback) throws UnsupportedEncodingException {

   String encodedQuery = URLEncoder.encode(cqlQuery, "UTF-8");

    String location =
      String.format("%s/item-storage/items?query=%s", storageAddress, encodedQuery) +
        String.format("&limit=%s&offset=%s", pagingParameters.limit,
          pagingParameters.offset);

    vertx.createHttpClient().getAbs(location.toString(),
      handleMultipleResults(resultCallback, failureCallback))
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Accept", "application/json")
      .end();
  }

  @Override
  public void update(Item item,
              Consumer<Success<Void>> completionCallback,
              Consumer<Failure> failureCallback) {

    String location = String.format("%s/item-storage/items/%s", storageAddress, item.id);

    Handler<HttpClientResponse> onResponse = response ->
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

    JsonObject itemToSend = mapToItemRequest(item);

    vertx.createHttpClient().requestAbs(HttpMethod.PUT, location, onResponse)
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "text/plain")
      .end(Json.encodePrettily(itemToSend));
  }

  @Override
  public void delete(String id,
              Consumer<Success<Void>> completionCallback,
              Consumer<Failure> failureCallback) {
    String location = String.format("%s/item-storage/items/%s", storageAddress, id);

    Handler<HttpClientResponse> onResponse = response ->
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

    vertx.createHttpClient().requestAbs(HttpMethod.DELETE, location, onResponse)
      .exceptionHandler(exceptionHandler(failureCallback))
      .putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token)
      .putHeader("Accept", "application/json, text/plain")
      .end();
  }

  private Item mapFromJson(JsonObject itemFromServer) {
    return new Item(
      itemFromServer.getString("id"),
      itemFromServer.getString("title"),
      itemFromServer.getString("barcode"),
      itemFromServer.getString("instanceId"),
      itemFromServer.getJsonObject("status").getString("name"),
      itemFromServer.getString("materialTypeId"),
      itemFromServer.getString("permanentLocationId"),
      itemFromServer.getString("temporaryLocationId"),
      itemFromServer.getString("permanentLoanTypeId"),
      itemFromServer.getString("temporaryLoanTypeId"));
  }

  private JsonObject mapToItemRequest(Item item) {
    JsonObject itemToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    itemToSend.put("id", item.id != null ? item.id : UUID.randomUUID().toString());
    itemToSend.put("title", item.title);
    itemToSend.put("status", new JsonObject().put("name", item.status));

    includeIfPresent(itemToSend, "barcode", item.barcode);
    includeIfPresent(itemToSend, "instanceId", item.instanceId);
    includeIfPresent(itemToSend, "materialTypeId", item.materialTypeId);
    includeIfPresent(itemToSend, "permanentLoanTypeId", item.permanentLoanTypeId);
    includeIfPresent(itemToSend, "temporaryLoanTypeId", item.temporaryLoanTypeId);
		includeIfPresent(itemToSend, "permanentLocationId", item.permanentLocationId);
		includeIfPresent(itemToSend, "temporaryLocationId", item.temporaryLocationId);

    return itemToSend;
  }

  private void includeIfPresent(
    JsonObject itemToSend,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      itemToSend.put(propertyName, propertyValue);
    }
  }

  private Handler<Throwable> exceptionHandler(Consumer<Failure> failureCallback) {
    return it -> failureCallback.accept(new Failure(it.getMessage(), null));
  }

  private Handler<HttpClientResponse> handleMultipleResults(
    Consumer<Success<MultipleRecords<Item>>> resultCallback,
    Consumer<Failure> failureCallback) {

    return response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 200) {
          JsonObject wrappedItems = new JsonObject(responseBody);

          List<JsonObject> items = JsonArrayHelper.toList(
            wrappedItems.getJsonArray("items"));

          List<Item> foundItems = items.stream()
            .map(this::mapFromJson)
            .collect(Collectors.toList());

          MultipleRecords<Item> result = new MultipleRecords<>(
            foundItems, wrappedItems.getInteger("totalRecords"));

          resultCallback.accept(new Success<>(result));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
  }
}
