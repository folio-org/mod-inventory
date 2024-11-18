package org.folio.inventory.storage.external;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.exceptions.InternalServerErrorException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.SynchronousHttpClient;

class ExternalStorageModuleInstanceCollection
  extends ExternalStorageModuleCollection<Instance>
  implements InstanceCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleInstanceCollection.class);

  private final String batchAddress;
  private SynchronousHttpClient httpClient;

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
      .toList();

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
  public Instance findByIdAndUpdate(String id, JsonObject instance, Context inventoryContext) {
    try {
      SynchronousHttpClient client = getSynchronousHttpClient(inventoryContext);
      var url = individualRecordLocation(id);
      var response = client.get(url);
      var responseBody = response.getBody();

      if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOGGER.warn("Instance not found by id - {} : {}",
          id, responseBody);
        throw new NotFoundException(
          String.format("Instance not found by id - %s : %s", id, responseBody));
      } else if (response.getStatusCode() != HttpStatus.SC_OK) {
        LOGGER.warn("Failed to fetch Instance by id - {} : {}, {}",
          id, responseBody, response.getStatusCode());
        throw new ExternalResourceFetchException("Failed to fetch Instance record",
          responseBody, response.getStatusCode(), null);
      }

      var jsonInstance = new JsonObject(responseBody);
      var existingInstance = mapFromJson(jsonInstance);
      var modified = modifyInstance(existingInstance, instance);
      var modifiedInstance = mapToRequest(modified);

      LOGGER.info("modifiedInstance: {}", modifiedInstance.encode());

      response = client.put(url, modifiedInstance);
      if (response.getStatusCode() != 204) {
        var errorMessage = String.format("Failed to update Instance by id - %s : %s, %s",
          id, response.getBody(), response.getStatusCode());
        LOGGER.warn(errorMessage);
        throw new InternalServerErrorException(errorMessage);
      }
      return modified;
    } catch (Exception ex) {
      throw new InternalServerErrorException(
        String.format("Failed to find and update Instance by id - %s : %s", id, ex));
    }
  }

  private Instance modifyInstance(Instance existingInstance, JsonObject instance) {
    instance.put(Instance.ID, existingInstance.getId());
    JsonObject existing = JsonObject.mapFrom(existingInstance);
    JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existing, instance);
    return Instance.fromJson(mergedInstanceAsJson);
  }

  private SynchronousHttpClient getSynchronousHttpClient(Context context) throws MalformedURLException {
    if (httpClient == null) {
      httpClient = new SynchronousHttpClient(new URL(context.getOkapiLocation()), tenant, token, context.getUserId(), null, null);
    }

    return httpClient;
  }
}
