package org.folio.inventory.storage.external;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;

class ExternalStorageModuleInstanceCollection
  extends ExternalStorageModuleCollection<Instance>
  implements InstanceCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleInstanceCollection.class);

  private final String batchAddress;

  ExternalStorageModuleInstanceCollection(
    String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "instance-storage/instances"),
      tenant, token, "instances", client);

    batchAddress = String.format("%s/%s", baseAddress, "instance-storage/batch/instances");
  }

  @Override
  protected JsonObject mapToRequest(Instance instance) {
    return instance.getJsonForStorage();
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    return Instance.fromJson(instanceFromServer)
      .setMetadata(new Metadata(instanceFromServer.getJsonObject("metadata")));
  }

  @Override
  protected String getId(Instance record) {
    return record.getId();
  }

  @Override
  public void addBatch(List<Instance> items,
    Consumer<Success<BatchResult<Instance>>> resultCallback, Consumer<Failure> failureCallback) {

    List<JsonObject> jsonList = items.stream()
      .map(this::mapToRequest)
      .collect(Collectors.toList());

    JsonObject batchRequest = new JsonObject()
      .put("instances", new JsonArray(jsonList))
      .put("totalRecords", jsonList.size());

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final HttpRequest<Buffer> request = withStandardHeaders(webClient.postAbs(batchAddress));

    request.sendJsonObject(batchRequest, futureResponse::complete);

    futureResponse
      .thenCompose(this::mapAsyncResultToCompletionStage)
      .thenAccept(response -> {
        if (isBatchResponse(response)) {
          try {
            JsonObject batchResponse = response.getJson();
            JsonArray createdInstances = batchResponse.getJsonArray("instances");

            List<Instance> instancesList = new ArrayList<>();
            for (int i = 0; i < createdInstances.size(); i++) {
              instancesList.add(mapFromJson(createdInstances.getJsonObject(i)));
            }
            BatchResult<Instance> batchResult = new BatchResult<>();
            batchResult.setBatchItems(instancesList);
            batchResult.setErrorMessages(batchResponse.getJsonArray("errorMessages").getList());

            resultCallback.accept(new Success<>(batchResult));
          } catch (Exception e) {
            LOGGER.error(e);
            failureCallback.accept(new Failure(e.getMessage(), response.getStatusCode()));
          }

        } else {
          failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });
  }

  private boolean isBatchResponse(Response response) {
    int statusCode = response.getStatusCode();
    String contentHeaderValue = response.getContentType();
    return statusCode == SC_CREATED
      || (statusCode == SC_INTERNAL_SERVER_ERROR && APPLICATION_JSON.equals(contentHeaderValue));
  }

  @Override
  public Future<Instance> findByIdAndUpdate(String id, Instance instance, Context context) {
    try {
      var client = java.net.http.HttpClient.newHttpClient();
      var getRequest = java.net.http.HttpRequest.newBuilder()
        .uri(URI.create(String.format("%s/%s/%s", context.getOkapiLocation(), storageAddress, id)))
        .headers(OKAPI_TOKEN_HEADER, context.getToken(),
          OKAPI_TENANT_HEADER, context.getTenantId(),
          OKAPI_URL_HEADER, context.getOkapiLocation())
        .GET()
        .build();

      var response = client.send(getRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
         LOGGER.info("body: {}", response.body());
         LOGGER.info("jsonObject: {}", new JsonObject(response.body()));
      }

      return Future.succeededFuture();
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Instance> updateInstance(Instance existingInstance, org.folio.Instance mappedInstance) {
    mappedInstance.setId(existingInstance.getId());
    JsonObject existing = JsonObject.mapFrom(existingInstance);
    JsonObject mapped = JsonObject.mapFrom(mappedInstance);
    JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existing, mapped);
    Instance mergedInstance = Instance.fromJson(mergedInstanceAsJson);
    return CompletableFuture.completedFuture(mergedInstance);
  }
}
