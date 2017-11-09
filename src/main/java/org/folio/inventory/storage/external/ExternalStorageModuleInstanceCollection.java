package org.folio.inventory.storage.external;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.Creator;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.support.JsonArrayHelper;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.folio.inventory.support.JsonArrayHelper.toList;

class ExternalStorageModuleInstanceCollection
  implements InstanceCollection {

  private final Vertx vertx;
  private final String storageAddress;
  private final String tenant;
  private final String token;

  public ExternalStorageModuleInstanceCollection(Vertx vertx,
                                              String storageAddress,
                                              String tenant,
                                              String token) {
      this.vertx = vertx;
      this.storageAddress = storageAddress;
      this.tenant = tenant;
      this.token = token;
    }

    @Override
    public void add(Instance item,
      Consumer<Success<Instance>> resultCallback,
      Consumer<Failure> failureCallback) {

      String location = storageAddress + "/instance-storage/instances";

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 201) {
            Instance createdInstance = mapFromJson(new JsonObject(responseBody));

            resultCallback.accept(new Success<>(createdInstance));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

      JsonObject itemToSend = mapToInstanceRequest(item);

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
      Consumer<Success<Instance>> resultCallback,
      Consumer<Failure> failureCallback) {

      String location = String.format("%s/instance-storage/instances/%s", storageAddress, id);

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          switch (statusCode) {
            case 200:
              JsonObject instanceFromServer = new JsonObject(responseBody);

              Instance foundInstance = mapFromJson(instanceFromServer);

              resultCallback.accept(new Success(foundInstance));
              break;

            case 404:
              resultCallback.accept(new Success(null));
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
                        Consumer<Success<Map>> resultCallback,
                        Consumer<Failure> failureCallback) {

      String location = String.format(storageAddress
          + "/instance-storage/instances?limit=%s&offset=%s",
        pagingParameters.limit, pagingParameters.offset);

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 200) {
            JsonObject wrappedItems = new JsonObject(responseBody);

            List<JsonObject> items = JsonArrayHelper.toList(
              wrappedItems.getJsonArray("instances"));

            List<Instance> foundItems = items.stream()
              .map(this::mapFromJson)
              .collect(Collectors.toList());

            Map<String, Object> result = new HashMap<>();

            result.put("instances", foundItems);
            result.put("totalRecords", wrappedItems.getInteger("totalRecords"));

            resultCallback.accept(new Success<>(result));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

      vertx.createHttpClient().requestAbs(HttpMethod.GET, location, onResponse)
        .exceptionHandler(exceptionHandler(failureCallback))
        .putHeader("X-Okapi-Tenant", tenant)
        .putHeader("X-Okapi-Token", token)
        .putHeader("Accept", "application/json")
        .end();
    }

    @Override
    public void empty(Consumer<Success> completionCallback,
      Consumer<Failure> failureCallback) {
      String location = storageAddress + "/instance-storage/instances";

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 204) {
            completionCallback.accept(new Success(null));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

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
      Consumer<Success<Map>> resultCallback,
      Consumer<Failure> failureCallback) throws UnsupportedEncodingException {

      String encodedQuery = URLEncoder.encode(cqlQuery, "UTF-8");

      String location =
        String.format("%s/instance-storage/instances?query=%s", storageAddress, encodedQuery) +
          String.format("&limit=%s&offset=%s", pagingParameters.limit,
            pagingParameters.offset);

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 200) {
            JsonObject wrappedItems = new JsonObject(responseBody);

            List<JsonObject> instances = toList(
              wrappedItems.getJsonArray("instances"));

            List<Instance> foundInstances = instances.stream()
              .map(this::mapFromJson)
              .collect(Collectors.toList());

            Map<String, Object> result = new HashMap<>();

            result.put("instances", foundInstances);
            result.put("totalRecords", wrappedItems.getInteger("totalRecords"));

            resultCallback.accept(new Success(result));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

      vertx.createHttpClient().getAbs(location.toString(), onResponse)
        .exceptionHandler(exceptionHandler(failureCallback))
        .putHeader("X-Okapi-Tenant", tenant)
        .putHeader("X-Okapi-Token", token)
        .putHeader("Accept", "application/json")
        .end();
    }

    @Override
    public void update(Instance item,
      Consumer<Success> completionCallback,
      Consumer<Failure> failureCallback) {

      String location = String.format("%s/instance-storage/instances/%s", storageAddress, item.id);

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 204) {
            completionCallback.accept(new Success(null));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

      JsonObject itemToSend = mapToInstanceRequest(item);

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
      Consumer<Success> completionCallback,
      Consumer<Failure> failureCallback) {
      String location = String.format("%s/instance-storage/instances/%s", storageAddress, id);

      Handler<HttpClientResponse> onResponse = response -> {
        response.bodyHandler(buffer -> {
          String responseBody = buffer.getString(0, buffer.length());
          int statusCode = response.statusCode();

          if(statusCode == 204) {
            completionCallback.accept(new Success(null));
          }
          else {
            failureCallback.accept(new Failure(responseBody, statusCode));
          }
        });
      };

      vertx.createHttpClient().requestAbs(HttpMethod.DELETE, location, onResponse)
        .exceptionHandler(exceptionHandler(failureCallback))
        .putHeader("X-Okapi-Tenant", tenant)
        .putHeader("X-Okapi-Token", token)
        .putHeader("Accept", "application/json, text/plain")
        .end();
    }

  private JsonObject mapToInstanceRequest(Instance instance) {
    JsonObject instanceToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    instanceToSend.put("id", instance.id != null ? instance.id : UUID.randomUUID().toString());
    instanceToSend.put("title", instance.title);
    includeIfPresent(instanceToSend, "instanceTypeId", instance.instanceTypeId);
    includeIfPresent(instanceToSend, "source", instance.source);
    instanceToSend.put("identifiers", instance.identifiers);
    instanceToSend.put("creators", instance.creators);

    return instanceToSend;
  }

  private Instance mapFromJson(JsonObject instanceFromServer) {

    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray("identifiers", new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it.getString("identifierTypeId"), it.getString("value")))
      .collect(Collectors.toList());

    List<JsonObject> creators = toList(
      instanceFromServer.getJsonArray("creators", new JsonArray()));

    List<Creator> mappedCreators = creators.stream()
      .map(it -> new Creator(it.getString("creatorTypeId"), it.getString("name")))
      .collect(Collectors.toList());

    return new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString("title"),
      mappedIdentifiers,
      instanceFromServer.getString("source"),
      instanceFromServer.getString("instanceTypeId"),
      mappedCreators);
  }

  private void includeIfPresent(
    JsonObject instanceToSend,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }

  private Handler<Throwable> exceptionHandler(Consumer<Failure> failureCallback) {
    return it -> failureCallback.accept(new Failure(it.getMessage(), null));
  }
}
