package org.folio.inventory.resources;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.RedirectResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class InstancesBatch extends AbstractInstances {

  private static final String INSTANCES_BATCH_PATH = INSTANCES_PATH + "/batch";
  public static final String BATCH_RESPONSE_FIELD_INSTANCES = "instances";
  public static final String BATCH_RESPONSE_FIELD_ERROR_MESSAGES = "errorMessages";
  public static final String BATCH_RESPONSE_FIELD_TOTAL_RECORDS = "totalRecords";

  public InstancesBatch(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  public void register(Router router) {
    router.post(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.put(INSTANCES_PATH + "*").handler(BodyHandler.create());

    router.post(INSTANCES_BATCH_PATH).handler(this::createBatch);
  }

  /**
   * Creates a collection of Instances expecting the request could be mapped to Instances schema.
   *
   * @param routingContext context for the handling of a request in Vert.x-Web
   */
  private void createBatch(RoutingContext routingContext) {
    WebContext webContext = new WebContext(routingContext);
    JsonObject requestBody = routingContext.getBodyAsJson();
    JsonArray instanceCollection = requestBody.getJsonArray(BATCH_RESPONSE_FIELD_INSTANCES, new JsonArray());
    log.info("Received batch of Instances, size:" + instanceCollection.size());

    Pair<List<JsonObject>, List<String>> validationResult = validateInstances(instanceCollection);
    List<JsonObject> validInstances = validationResult.getLeft();
    List<String> errorMessages = validationResult.getRight();

    if (validInstances.isEmpty()) {
      respondWithErrorMessages(errorMessages, routingContext);
    } else {
      setInstancesIdIfNecessary(validInstances);
      List<Instance> instancesToCreate = validInstances.stream()
        .map(this::requestToInstance)
        .collect(Collectors.toList());

      storage.getInstanceCollection(webContext).addBatch(instancesToCreate, success -> {
          BatchResult<Instance> batchResult = success.getResult();
          List<Instance> createdInstances = batchResult.getBatchItems();
          errorMessages.addAll(batchResult.getErrorMessages());

          log.info(format("Was created instances from batch %d/%d", createdInstances.size(),
            requestBody.getInteger(BATCH_RESPONSE_FIELD_TOTAL_RECORDS)));

          if (!createdInstances.isEmpty()) {
            updateRelatedRecords(validInstances, createdInstances, routingContext, webContext).
              setHandler(ar -> {
                JsonObject responseBody = getBatchResponse(createdInstances, errorMessages, webContext);
                RedirectResponse.created(routingContext.response(), Buffer.buffer(responseBody.encodePrettily()));
              });
          } else {
            JsonObject responseBody = getBatchResponse(createdInstances, errorMessages, webContext);
            RedirectResponse.serverError(routingContext.response(), Buffer.buffer(responseBody.encodePrettily()));
          }
        },
        failure -> {
          RedirectResponse.serverError(routingContext.response(), Buffer.buffer(failure.getReason()));
          log.error("All the Instances from batch were not created, cause:" + failure.getReason());
        });
    }
  }

  /**
   * Performs instances validation and returns pair (tuple) of valid instances list
   * and list of error messages.
   *
   * @param instances instances to validate
   * @return pair (tuple) of valid instances list and list of error messages
   */
  private Pair<List<JsonObject>, List<String>> validateInstances(JsonArray instances) {
    List<JsonObject> validInstances = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    for (int i = 0; i < instances.size(); i++) {
      List<String> validationMessages = validateInstance(instances.getJsonObject(i));
      if (validationMessages.isEmpty()) {
        validInstances.add(instances.getJsonObject(i));
      } else {
        errorMessages.add("Instance is not valid for further processing: " + validationMessages);
      }
    }
    return Pair.of(validInstances, errorMessages);
  }

  /**
   * Performs validation for incoming Instance json object
   *
   * @param jsonInstance Instance json object
   * @return error message
   */
  private List<String> validateInstance(JsonObject jsonInstance) {
    List<String> errorMessages = new ArrayList<>();
    if (StringUtils.isBlank(jsonInstance.getString(Instance.TITLE_KEY))) {
      String errorMessage = "Title must be provided for an instance: " + jsonInstance.getString("id");
      log.error(errorMessage);
      errorMessages.add(errorMessage);
    }
    return errorMessages;
  }

  /**
   * Sends response with specified error messages.
   *
   * @param errorMessages  error messages
   * @param routingContext routing context
   */
  private void respondWithErrorMessages(List<String> errorMessages, RoutingContext routingContext) {
    JsonObject responseBody = new JsonObject()
      .put(BATCH_RESPONSE_FIELD_ERROR_MESSAGES, new JsonArray(errorMessages))
      .put(BATCH_RESPONSE_FIELD_INSTANCES, new JsonArray())
      .put(BATCH_RESPONSE_FIELD_TOTAL_RECORDS, 0);
    RedirectResponse.serverError(routingContext.response(), Buffer.buffer(responseBody.encodePrettily()));
  }

  private JsonObject getBatchResponse(List<Instance> createdInstances, List<String> errorMessages, WebContext webContext) {
    List<JsonObject> jsonInstances = createdInstances.stream()
      .map(instance -> toRepresentation(instance, new ArrayList<>(), new ArrayList<>(),
        instance.getPrecedingTitles(), instance.getSucceedingTitles(), webContext))
      .collect(Collectors.toList());

    return new JsonObject()
      .put(BATCH_RESPONSE_FIELD_INSTANCES, new JsonArray(jsonInstances))
      .put(BATCH_RESPONSE_FIELD_ERROR_MESSAGES, new JsonArray(errorMessages))
      .put(BATCH_RESPONSE_FIELD_TOTAL_RECORDS, createdInstances.size());
  }

  /**
   * Sets id to each instance if instance have not.
   *
   * @param instances instances
   */
  private void setInstancesIdIfNecessary(List<JsonObject> instances) {
    instances.forEach(instance -> {
      if (isEmpty(instance.getString("id"))) {
        instance.put("id", UUID.randomUUID().toString());
      }
    });
  }

  /**
   * Updates relationships for specified created instances.
   * @param newInstances the new instances containing relationship arrays to persist.
   * @param createdInstances instances from storage whose relationships will be updated.
   * @param routingContext routingContext
   * @param webContext webContext
   */
  private Future<CompositeFuture> updateRelatedRecords(List<JsonObject> newInstances, List<Instance> createdInstances,
    RoutingContext routingContext, WebContext webContext) {

    Future<CompositeFuture> resultFuture = Future.future();
    try {
      Map<String, Instance> mapInstanceById = newInstances.stream()
        .collect(Collectors.toMap(instance -> instance.getString("id"), this::requestToInstance));

      List<Future> updateRelationshipsFutures = new ArrayList<>();
      for (Instance createdInstance : createdInstances) {
        Instance newInstance = mapInstanceById.get(createdInstance.getId());
        if (newInstance != null) {
          createdInstance.setParentInstances(newInstance.getParentInstances());
          createdInstance.setChildInstances(newInstance.getChildInstances());
          createdInstance.setPrecedingTitles(newInstance.getPrecedingTitles());
          createdInstance.setSucceedingTitles(newInstance.getSucceedingTitles());
          Future updateFuture = Future.future();
          updateRelationshipsFutures.add(updateFuture);
          updateRelatedRecords(routingContext, webContext, createdInstance, o -> updateFuture.complete());
        }
      }
      resultFuture.handle(CompositeFuture.join(updateRelationshipsFutures));
    } catch (IllegalStateException e) {
      log.error("Can not update instances relationships cause: " + e);
      resultFuture.fail(e);
    }
    return resultFuture;
  }
}
