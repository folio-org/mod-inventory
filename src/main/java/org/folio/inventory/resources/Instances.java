package org.folio.inventory.resources;

import static io.netty.util.internal.StringUtil.COMMA;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;
import static org.folio.inventory.support.EndpointFailureHandler.doExceptionally;
import static org.folio.inventory.support.EndpointFailureHandler.getKnownException;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.http.server.SuccessResponse.noContent;
import static org.folio.inventory.validation.InstancesValidators.refuseWhenHridChanged;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.HttpStatus;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.InstanceRelationship;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.services.InstanceRelationshipsService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.RedirectResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.validation.InstancePrecedingSucceedingTitleValidators;
import org.folio.inventory.validation.InstancesValidators;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.rest.client.SourceStorageRecordsClient;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class Instances extends AbstractInstances {
  private static final String INSTANCES_CONTEXT_PATH = INSTANCES_PATH + "/context";
  private static final String BLOCKED_FIELDS_CONFIG_PATH = INVENTORY_PATH + "/config/instances/blocked-fields";
  private static final String BLOCKED_FIELDS_UPDATE_ERROR_MESSAGE = "Instance is controlled by MARC record, "
    + "these fields are blocked and can not be updated: ";

  public Instances(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  public void register(Router router) {
    router.post(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.put(INSTANCES_PATH + "*").handler(BodyHandler.create());

    router.get(INSTANCES_CONTEXT_PATH).handler(this::getMetadataContext);
    router.get(BLOCKED_FIELDS_CONFIG_PATH).handler(this::getBlockedFieldsConfig);

    router.get(INSTANCES_PATH).handler(this::getAll);
    router.post(INSTANCES_PATH).handler(this::create);
    router.delete(INSTANCES_PATH).handler(this::deleteAll);

    router.get(INSTANCES_PATH + "/:id").handler(this::getById);
    router.put(INSTANCES_PATH + "/:id").handler(this::update);
    router.delete(INSTANCES_PATH + "/:id").handler(this::deleteById);
  }

  private void getMetadataContext(RoutingContext routingContext) {
    JsonObject representation = new JsonObject();

    representation.put("@context", new JsonObject()
      .put("dcterms", "http://purl.org/dc/terms/")
      .put(Instance.TITLE_KEY, "dcterms:title"));

    JsonResponse.success(routingContext.response(), representation);
  }

  private void getBlockedFieldsConfig(RoutingContext routingContext) {
    JsonObject response = new JsonObject();
    response.put("blockedFields", new JsonArray(Json.encode(config.getInstanceBlockedFields())));
    JsonResponse.success(routingContext.response(), response);
  }

  private void getAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String search = context.getStringParameter("query", null);

    PagingParameters pagingParameters = PagingParameters.from(context);

    if (pagingParameters == null) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "limit and offset must be numeric when supplied");
      return;
    }

    if (search == null) {
      storage.getInstanceCollection(context).findAll(
        pagingParameters,
        (Success<MultipleRecords<Instance>> success) -> makeInstancesResponse(success, routingContext, context),
        FailureResponseConsumer.serverError(routingContext.response())
      );
    } else {
      try {
        storage.getInstanceCollection(context).findByCql(
          search,
          pagingParameters,
          success -> makeInstancesResponse(success, routingContext, context),
          FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
    }
  }

  private void makeInstancesResponse(Success<MultipleRecords<Instance>> success,
    RoutingContext routingContext, WebContext context) {

    InstancesResponse instancesResponse = new InstancesResponse();
    instancesResponse.setSuccess(success);

    completedFuture(instancesResponse)
      .thenCompose(response -> fetchRelationships(response, routingContext))
      .thenCompose(response -> fetchPrecedingSucceedingTitles(response, routingContext, context))
      .whenComplete((result, ex) -> {
        if (ex == null) {
          JsonResponse.success(routingContext.response(),
            toRepresentation(result, context));
        } else {
          log.warn("Exception occurred", ex);
          handleFailure(getKnownException(ex), routingContext);
        }
      });
  }

  private void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject instanceRequest = routingContext.getBodyAsJson();

    if (StringUtils.isBlank(instanceRequest.getString(Instance.TITLE_KEY))) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Title must be provided for an instance");
      return;
    }

    Instance newInstance = InstanceUtil.jsonToInstance(instanceRequest);

    completedFuture(newInstance)
      .thenCompose(InstancePrecedingSucceedingTitleValidators::refuseWhenUnconnectedHasNoTitle)
      .thenCompose(instance -> storage.getInstanceCollection(context).add(instance))
      .thenCompose(response -> {
        response.setParentInstances(newInstance.getParentInstances());
        response.setChildInstances(newInstance.getChildInstances());
        response.setPrecedingTitles(newInstance.getPrecedingTitles());
        response.setSucceedingTitles(newInstance.getSucceedingTitles());

        return updateRelatedRecords(routingContext, context, response).thenApply(notUsed -> response);
      }).thenAccept(response -> {
        try {
          URL url = context.absoluteUrl(format("%s/%s",
            INSTANCES_PATH, response.getId()));
          RedirectResponse.created(routingContext.response(), url.toString());
        } catch (MalformedURLException e) {
          log.warn(
            format("Failed to create self link for instance: %s", e.toString()));
        }
      }).exceptionally(doExceptionally(routingContext));
  }

  private void update(RoutingContext rContext) {
    WebContext wContext = new WebContext(rContext);
    JsonObject instanceRequest = rContext.getBodyAsJson();
    Instance updatedInstance = InstanceUtil.jsonToInstance(instanceRequest);
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);

    completedFuture(updatedInstance)
      .thenCompose(InstancePrecedingSucceedingTitleValidators::refuseWhenUnconnectedHasNoTitle)
      .thenCompose(instance -> instanceCollection.findById(rContext.request().getParam("id")))
      .thenCompose(InstancesValidators::refuseWhenInstanceNotFound)
      .thenCompose(existingInstance -> refuseWhenBlockedFieldsChanged(existingInstance, updatedInstance))
      .thenCompose(existingInstance -> refuseWhenHridChanged(existingInstance, updatedInstance))
      .thenAccept(existingInstance -> updateInstance(updatedInstance, rContext, wContext))
      .exceptionally(doExceptionally(rContext));
  }

  /**
   * Call Source record storage to update suppress from discovery flag in underlying record
   *
   * @param wContext        - webContext
   * @param updatedInstance - Updated instance entity
   */
  private void updateSuppressFromDiscoveryFlag(WebContext wContext, Instance updatedInstance) {
    try {
      SourceStorageRecordsClient client = new SourceStorageRecordsClient(wContext.getOkapiLocation(),
        wContext.getTenantId(), wContext.getToken());
      client.putSourceStorageRecordsSuppressFromDiscoveryById(updatedInstance.getId(), "INSTANCE", updatedInstance.getDiscoverySuppress(), httpClientResponse -> {
        if (httpClientResponse.result().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          log.info(format("Suppress from discovery flag was successfully updated for record in SRS. InstanceID: %s",
            updatedInstance.getId()));
        } else {
          log.error(format("Suppress from discovery wasn't changed for SRS record. InstanceID: %s StatusCode: %s",
            updatedInstance.getId(), httpClientResponse.result().statusCode()));
        }
      });
    } catch (Exception e) {
      log.error("Error during updating suppress from discovery flag for record in SRS", e);
    }
  }

  /**
   * Returns true if given Instance has linked record in source-record-storage
   *
   * @param instance given instance
   * @return boolean
   */
  private boolean isInstanceControlledByRecord(Instance instance) {
    return "MARC".equals(instance.getSource());
  }

  /**
   * Updates given Instance
   *
   * @param instance instance for update
   * @param rContext routing context
   * @param wContext web context
   */
  private void updateInstance(Instance instance, RoutingContext rContext, WebContext wContext) {
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);
    instanceCollection.update(
      instance,
      v -> {
        updateRelatedRecords(rContext, wContext, instance)
          .whenComplete((result, ex) -> {
            if (ex != null) {
              log.warn("Exception occurred", ex);
              handleFailure(getKnownException(ex), rContext);
            } else {
              noContent(rContext.response());
            }
          });
        if (isInstanceControlledByRecord(instance)) {
          updateSuppressFromDiscoveryFlag(wContext, instance);
        }
      },
      FailureResponseConsumer.serverError(rContext.response()));
  }

  /**
   * Compares existing instance with it's version for update,
   * returns true if blocked fields are changed
   *
   * @param existingInstance instance that exists in database
   * @param updatedInstance  instance with changes for update
   * @return boolean
   */
  private boolean areInstanceBlockedFieldsChanged(Instance existingInstance, Instance updatedInstance) {
    JsonObject existingInstanceJson = JsonObject.mapFrom(existingInstance);
    JsonObject updatedInstanceJson = JsonObject.mapFrom(updatedInstance);
    Map<String, Object> existingBlockedFields = new HashMap<>();
    Map<String, Object> updatedBlockedFields = new HashMap<>();
    for (String blockedFieldCode : config.getInstanceBlockedFields()) {
      existingBlockedFields.put(blockedFieldCode, existingInstanceJson.getValue(blockedFieldCode));
      updatedBlockedFields.put(blockedFieldCode, updatedInstanceJson.getValue(blockedFieldCode));
    }
    return ObjectUtils.notEqual(existingBlockedFields, updatedBlockedFields);
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).empty(
      v -> noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).delete(
      routingContext.request().getParam("id"),
      v -> noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void getById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).findById(
      routingContext.request().getParam("id"),
      it -> {
        Instance instance = it.getResult();
        if (instance != null) {
          completedFuture(instance)
            .thenCompose(response -> fetchInstanceRelationships(it, routingContext, context))
            .thenCompose(response -> fetchPrecedingSucceedingTitles(it, routingContext, context))
            .thenAccept(response -> successResponse(routingContext, context, response));
        } else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }


  /**
   * Fetches instance relationships for multiple Instance records, populates, responds
   *
   * @param instancesResponse Multi record Instances result
   * @param routingContext
   */
  private CompletableFuture<InstancesResponse> fetchRelationships(
    InstancesResponse instancesResponse,
    RoutingContext routingContext) {

    final List<String> instanceIds =
      getInstanceIdsFromInstanceResult(instancesResponse.getSuccess());

    return createInstanceRelationshipsService(routingContext)
      .fetchInstanceRelationships(instanceIds)
      .thenCompose(response -> withInstancesRelationships(instancesResponse, response));
  }

  private CompletableFuture<InstancesResponse> fetchPrecedingSucceedingTitles(
    InstancesResponse instancesResponse, RoutingContext routingContext, WebContext context) {

    List<String> instanceIds = getInstanceIdsFromInstanceResult(instancesResponse.getSuccess());

    return createInstanceRelationshipsService(routingContext)
      .fetchInstancePrecedingSucceedingTitles(instanceIds)
      .thenCompose(response ->
        withPrecedingSucceedingTitles(routingContext, context, instancesResponse, response));
  }

  /**
   * Fetches instance relationships for a single Instance result, populates, responds
   *
   * @param success        Single record Instance result
   * @param routingContext
   * @param context
   */
  private CompletableFuture<Instance> fetchInstanceRelationships(
    Success<Instance> success, RoutingContext routingContext, WebContext context) {

    Instance instance = success.getResult();
    List<String> instanceIds = getInstanceIdsFromInstanceResult(success);
    String query = createQueryForRelatedInstances(instanceIds);
    CollectionResourceClient relatedInstancesClient =
      createInstanceRelationshipsClient(routingContext, context);

    if (relatedInstancesClient != null) {
      CompletableFuture<Response> relatedInstancesFetched = new CompletableFuture<>();

      relatedInstancesClient.getMany(query, relatedInstancesFetched::complete);

      return relatedInstancesFetched
        .thenCompose(response -> withInstanceRelationships(instance, response));
    }
    return completedFuture(null);
  }

  private void successResponse(RoutingContext routingContext, WebContext context,
    Instance instance) {

    JsonResponse.success(routingContext.response(), toRepresentation(instance,
      instance.getParentInstances(), instance.getChildInstances(),
      instance.getPrecedingTitles(), instance.getSucceedingTitles(), context));
  }

  private CompletableFuture<InstancesResponse> withInstancesRelationships(
    InstancesResponse instancesResponse, List<JsonObject> relationsList) {

    Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap<>();
    Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap<>();

    relationsList.forEach(rel -> {
      addToList(childInstanceMap, rel.getString("superInstanceId"), new InstanceRelationshipToChild(rel));
      addToList(parentInstanceMap, rel.getString("subInstanceId"), new InstanceRelationshipToParent(rel));
    });

    instancesResponse.setChildInstanceMap(childInstanceMap);
    instancesResponse.setParentInstanceMap(parentInstanceMap);

    return CompletableFuture.completedFuture(instancesResponse);
  }

  private CompletableFuture<Instance> withInstanceRelationships(Instance instance,
    Response result) {

    List<InstanceRelationshipToParent> parentInstanceList = new ArrayList<>();
    List<InstanceRelationshipToChild> childInstanceList = new ArrayList<>();
    if (result.getStatusCode() == 200) {
      JsonObject json = result.getJson();
      List<JsonObject> relationsList = JsonArrayHelper.toList(json.getJsonArray("instanceRelationships"));
      relationsList.forEach(rel -> {
        if (rel.getString(InstanceRelationship.SUPER_INSTANCE_ID_KEY).equals(instance.getId())) {
          childInstanceList.add(new InstanceRelationshipToChild(rel));
        } else if (rel.getString(InstanceRelationship.SUB_INSTANCE_ID_KEY).equals(instance.getId())) {
          parentInstanceList.add(new InstanceRelationshipToParent(rel));
        }
      });
      instance.getParentInstances().addAll(parentInstanceList);
      instance.getChildInstances().addAll(childInstanceList);
    }
    return completedFuture(instance);
  }


  private CompletableFuture<Instance> fetchPrecedingSucceedingTitles(
    Success<Instance> success, RoutingContext routingContext, WebContext context) {

    Instance instance = success.getResult();
    List<String> instanceIds = getInstanceIdsFromInstanceResult(success);
    String queryForPrecedingSucceedingInstances = createQueryForPrecedingSucceedingInstances(instanceIds);
    CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(routingContext, context);

    CompletableFuture<Response> precedingSucceedingTitlesFetched = new CompletableFuture<>();

    precedingSucceedingTitlesClient.getMany(queryForPrecedingSucceedingInstances, precedingSucceedingTitlesFetched::complete);

    return precedingSucceedingTitlesFetched
      .thenCompose(response ->
        withPrecedingSucceedingTitles(routingContext, context, instance, response));
  }

  // Utilities

  private List<String> getInstanceIdsFromInstanceResult(Success success) {
    List<String> instanceIds = new ArrayList<>();
    if (success.getResult() instanceof Instance) {
      instanceIds = Collections.singletonList(((Instance) success.getResult()).getId());
    } else if (success.getResult() instanceof MultipleRecords) {
      instanceIds = (((MultipleRecords<Instance>) success.getResult()).records.stream()
        .map(Instance::getId)
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList()));
    }
    return instanceIds;
  }

  private synchronized <T> void addToList(Map<String, List<T>> items,
    String mapKey, T myItem) {

    List<T> itemsList = items.get(mapKey);

    // if list does not exist create it
    if (itemsList == null) {
      itemsList = new ArrayList<>();
      itemsList.add(myItem);
      items.put(mapKey, itemsList);
    } else {
      // add if item is not already in list
      if (!itemsList.contains(myItem)) {
        itemsList.add(myItem);
      }
    }
  }

  private CompletableFuture<InstancesResponse> withPrecedingSucceedingTitles(
    RoutingContext routingContext, WebContext context,
    InstancesResponse instancesResponse, List<JsonObject> relationsList) {

    Map<String, List<CompletableFuture<PrecedingSucceedingTitle>>> precedingTitlesMap = new HashMap<>();
    Map<String, List<CompletableFuture<PrecedingSucceedingTitle>>> succeedingTitlesMap = new HashMap<>();

    relationsList.forEach(rel -> {
      final String precedingInstanceId = rel.getString(PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY);
      if (StringUtils.isNotBlank(precedingInstanceId)) {
        addToList(precedingTitlesMap, precedingInstanceId, getPrecedingSucceedingTitle(routingContext, context, rel,
          PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY));
      }
      final String succeedingInstanceId = rel.getString(PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY);
      if (StringUtils.isNotBlank(succeedingInstanceId)) {
        addToList(succeedingTitlesMap, succeedingInstanceId, getPrecedingSucceedingTitle(routingContext, context, rel,
          PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY));
      }
    });

    return completedFuture(instancesResponse)
      .thenCompose(r -> withPrecedingTitles(instancesResponse, precedingTitlesMap))
      .thenCompose(r -> withSucceedingTitles(instancesResponse, succeedingTitlesMap));
  }

  private CompletableFuture<Instance> withPrecedingSucceedingTitles(
    RoutingContext routingContext, WebContext context, Instance instance,
    Response result) {

    if (result.getStatusCode() == 200) {
      JsonObject json = result.getJson();
      List<JsonObject> relationsList = JsonArrayHelper.toList(json.getJsonArray("precedingSucceedingTitles"));

      List<CompletableFuture<PrecedingSucceedingTitle>> precedingTitleCompletableFutures =
        relationsList.stream().filter(rel -> isPrecedingTitle(instance, rel))
          .map(rel -> getPrecedingSucceedingTitle(routingContext, context, rel,
            PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY)).collect(Collectors.toList());

      List<CompletableFuture<PrecedingSucceedingTitle>> succeedingTitleCompletableFutures =
        relationsList.stream().filter(rel -> isSucceedingTitle(instance, rel))
          .map(rel -> getPrecedingSucceedingTitle(routingContext, context, rel,
            PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY)).collect(Collectors.toList());

      return completedFuture(instance)
        .thenCompose(r -> withPrecedingTitles(instance, precedingTitleCompletableFutures))
        .thenCompose(r -> withSucceedingTitles(instance, succeedingTitleCompletableFutures));
    }
    return completedFuture(null);
  }

  private boolean isPrecedingTitle(Instance instance, JsonObject rel) {
    return instance.getId().equals(
      rel.getString(PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY));
  }

  private boolean isSucceedingTitle(Instance instance, JsonObject rel) {
    return instance.getId().equals(
      rel.getString(PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY));
  }

  private CompletionStage<Instance> withSucceedingTitles(Instance instance,
    List<CompletableFuture<PrecedingSucceedingTitle>> succeedingTitleCompletableFutures) {

    return allResultsOf(succeedingTitleCompletableFutures)
      .thenApply(instance::setSucceedingTitles);
  }

  private CompletableFuture<Instance> withPrecedingTitles(Instance instance,
    List<CompletableFuture<PrecedingSucceedingTitle>> precedingTitleCompletableFutures) {

    return allResultsOf(precedingTitleCompletableFutures)
      .thenApply(instance::setPrecedingTitles);
  }

  private CompletableFuture<InstancesResponse> withPrecedingTitles(
    InstancesResponse instance,
    Map<String, List<CompletableFuture<PrecedingSucceedingTitle>>> precedingTitles) {

    return mapToCompletableFutureMap(precedingTitles)
      .thenApply(instance::setPrecedingTitlesMap);
  }

  private CompletableFuture<InstancesResponse> withSucceedingTitles(
    InstancesResponse instance,
    Map<String, List<CompletableFuture<PrecedingSucceedingTitle>>> precedingTitles) {

    return mapToCompletableFutureMap(precedingTitles)
      .thenApply(instance::setSucceedingTitlesMap);
  }

  private CompletableFuture<Map<String, List<PrecedingSucceedingTitle>>> mapToCompletableFutureMap(
    Map<String, List<CompletableFuture<PrecedingSucceedingTitle>>> precedingTitles) {

    Map<String, CompletableFuture<List<PrecedingSucceedingTitle>>> map =
      precedingTitles.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> allResultsOf(e.getValue())));

    return CompletableFuture.allOf(map.values().toArray(new CompletableFuture[0]))
      .thenApply(r -> map.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().join())));
  }

  private CompletableFuture<PrecedingSucceedingTitle> getPrecedingSucceedingTitle(
    RoutingContext routingContext, WebContext context,
    JsonObject rel, String precedingSucceedingKey) {

    if (!StringUtils.isBlank(rel.getString(precedingSucceedingKey))) {
      String precedingInstanceId = rel.getString(precedingSucceedingKey);
      CompletableFuture<Success<Instance>> getInstanceFuture = new CompletableFuture<>();
      storage
        .getInstanceCollection(context).findById(precedingInstanceId, getInstanceFuture::complete,
        FailureResponseConsumer.serverError(routingContext.response()));

      return getInstanceFuture.thenApply(response -> {
        Instance precedingInstance = response.getResult();
        if (precedingInstance != null) {
          return PrecedingSucceedingTitle.from(rel,
            precedingInstance.getTitle(),
            precedingInstance.getHrid(),
            new JsonArray(precedingInstance.getIdentifiers()));
        } else {
          return null;
        }
      });
    } else {
      return completedFuture(PrecedingSucceedingTitle.from(rel));
    }
  }

  private InstanceRelationshipsService createInstanceRelationshipsService(RoutingContext routingContext) {
    final WebContext webContext = new WebContext(routingContext);

    return new InstanceRelationshipsService(
      createInstanceRelationshipsClient(routingContext, webContext),
      createPrecedingSucceedingTitlesClient(routingContext, webContext));
  }

  private CompletionStage<Instance> refuseWhenBlockedFieldsChanged(
    Instance existingInstance, Instance updatedInstance) {

    if (isInstanceControlledByRecord(existingInstance)
      && areInstanceBlockedFieldsChanged(existingInstance, updatedInstance)) {

      String errorMessage = BLOCKED_FIELDS_UPDATE_ERROR_MESSAGE + StringUtils
        .join(config.getInstanceBlockedFields(), COMMA);

      log.error(errorMessage);
      return failedFuture(new UnprocessableEntityException(errorMessage, null, null));
    }

    return completedFuture(existingInstance);
  }
}
