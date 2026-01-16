package org.folio.inventory.resources;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.support.EndpointFailureHandler.getKnownException;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.validation.InstancePrecedingSucceedingTitleValidators.isTitleMissingForUnconnectedPrecedingSucceeding;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.server.RedirectResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class InstancesBatch extends AbstractInstances {

  private static final String INSTANCES_BATCH_PATH = INSTANCES_PATH + "/batch";
  public static final String BATCH_RESPONSE_FIELD_INSTANCES = "instances";
  public static final String BATCH_RESPONSE_FIELD_ERROR_MESSAGES = "errorMessages";
  public static final String BATCH_RESPONSE_FIELD_TOTAL_RECORDS = "totalRecords";

  public InstancesBatch(final Storage storage, final HttpClient client, final ConsortiumService consortiumService) {
    super(storage, client, consortiumService);
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
    JsonObject requestBody = routingContext.body().asJsonObject();
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
        .map(Instance::fromJson)
        .collect(Collectors.toList());

      storage.getInstanceCollection(webContext).addBatch(instancesToCreate, success -> {
          BatchResult<Instance> batchResult = success.getResult();
          List<Instance> createdInstances = batchResult.getBatchItems();
          errorMessages.addAll(batchResult.getErrorMessages());

          log.info(format("Was created instances from batch %d/%d", createdInstances.size(),
            requestBody.getInteger(BATCH_RESPONSE_FIELD_TOTAL_RECORDS)));

          if (!createdInstances.isEmpty()) {
            updateRelatedRecords(validInstances, createdInstances, routingContext, webContext).
              onComplete(ar -> {
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

    final List<PrecedingSucceedingTitle> precedingTitles = toList(jsonInstance
      .getJsonArray(Instance.PRECEDING_TITLES_KEY), PrecedingSucceedingTitle::from);
    final List<PrecedingSucceedingTitle> succeedingTitles = toList(jsonInstance
      .getJsonArray(Instance.SUCCEEDING_TITLES_KEY), PrecedingSucceedingTitle::from);

    if (isTitleMissingForUnconnectedPrecedingSucceeding(precedingTitles)) {
      errorMessages.add("Title is required for unconnected preceding title");
    }

    if (isTitleMissingForUnconnectedPrecedingSucceeding(succeedingTitles)) {
      errorMessages.add("Title is required for unconnected succeeding title");
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
      .map(instance -> instance.getJsonForResponse(webContext))
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
   *
   * @param newInstances     the new instances containing relationship arrays to persist.
   * @param createdInstances instances from storage whose relationships will be updated.
   * @param routingContext   routingContext
   * @param webContext       webContext
   */
  private Future<CompositeFuture> updateRelatedRecords(List<JsonObject> newInstances, List<Instance> createdInstances,
                                                       RoutingContext routingContext, WebContext webContext) {
    try {
      Map<String, Instance> mapInstanceById = newInstances.stream()
        .collect(Collectors.toMap(instance -> instance.getString("id"), Instance::fromJson));

      List<Future<?>> updateRelationshipsFutures = new ArrayList<>();
      for (Instance createdInstance : createdInstances) {
        Instance newInstance = mapInstanceById.get(createdInstance.getId());
        if (newInstance != null) {
          createdInstance.setParentInstances(newInstance.getParentInstances());
          createdInstance.setChildInstances(newInstance.getChildInstances());
          createdInstance.setPrecedingTitles(newInstance.getPrecedingTitles());
          createdInstance.setSucceedingTitles(newInstance.getSucceedingTitles());
          Promise<Void> updatePromise = Promise.promise();
          updateRelationshipsFutures.add(updatePromise.future());
          updateRelatedRecords(routingContext, webContext, createdInstance)
            .whenComplete((result, ex) -> {
              if (ex == null) {
                updatePromise.complete();
              } else {
                log.warn("Exception occurred", ex);
                handleFailure(getKnownException(ex), routingContext);
              }
            });
        }
      }
      return Future.join(updateRelationshipsFutures);
    } catch (IllegalStateException e) {
      log.error("Can not update instances relationships cause: " + e);
      return Future.failedFuture(e);
    }
  }
}
