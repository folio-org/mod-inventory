package org.folio.inventory.resources;

import static io.netty.util.internal.StringUtil.COMMA;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.commons.lang3.BooleanUtils.isFalse;
import static org.apache.commons.lang3.BooleanUtils.isTrue;
import static org.apache.commons.lang3.ObjectUtils.notEqual;
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

import org.apache.commons.lang3.StringUtils;
import org.folio.HttpStatus;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.InstanceRelationship;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.exceptions.InternalServerErrorException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.services.InstanceRelationshipsService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.RedirectResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.validation.InstancePrecedingSucceedingTitleValidators;
import org.folio.inventory.validation.InstancesValidators;
import org.folio.rest.client.SourceStorageRecordsClient;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;


public class Instances extends AbstractInstances {
  public static final String SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE = "staffSuppress and discoverySuppress cannot be set to false if instance is marked as deleted";
  private static final String BLOCKED_FIELDS_UPDATE_ERROR_MESSAGE = "Instance is controlled by MARC record, these fields are blocked and can not be updated: ";
  private static final String ID = "id";
  private static final String INSTANCE_ID = "instanceId";
  public static final String INSTANCE_ID_TYPE = "INSTANCE";

  public Instances(final Storage storage, final HttpClient client, final ConsortiumService consortiumService) {
    super(storage, client, consortiumService);
  }

  public void register(Router router) {
    router.post(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.put(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.patch(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.get(INSTANCES_PATH).handler(this::getAll);
    router.post(INSTANCES_PATH).handler(this::create);
    router.delete(INSTANCES_PATH).handler(this::deleteAll);
    router.get(INSTANCES_PATH + "/:id").handler(this::getById);
    router.put(INSTANCES_PATH + "/:id").handler(this::update);
    router.patch(INSTANCES_PATH + "/:id").handler(this::patch);
    router.delete(INSTANCES_PATH + "/:id").handler(this::deleteById);
    router.delete(INSTANCES_PATH + "/:id" + "/mark-deleted").handler(this::softDelete);
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
      .thenCompose(response -> lookUpBoundWithsForInstanceRecordSet(instancesResponse, routingContext, context))
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

    JsonObject instanceRequest = routingContext.body().asJsonObject();

    if (StringUtils.isBlank(instanceRequest.getString(Instance.TITLE_KEY))) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Title must be provided for an instance");
      return;
    }

    Instance newInstance = Instance.fromJson(instanceRequest);

    completedFuture(newInstance)
      .thenCompose(this::refuseWhenSuppressFlagsInvalid)
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
          RedirectResponse.created(routingContext.response(), url.toString(), response.getJsonForResponse(context));
        } catch (MalformedURLException e) {
          log.warn("Failed to create self link for instance, cause:", e);
        }
        }).exceptionally(doExceptionally(routingContext));
  }

  private void update(RoutingContext rContext) {
    WebContext wContext = new WebContext(rContext);
    JsonObject instanceRequest = rContext.body().asJsonObject();
    Instance updatedInstance = Instance.fromJson(instanceRequest);
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);

    completedFuture(updatedInstance)
      .thenCompose(this::refuseWhenSuppressFlagsInvalid)
      .thenCompose(InstancePrecedingSucceedingTitleValidators::refuseWhenUnconnectedHasNoTitle)
      .thenCompose(instance -> instanceCollection.findById(rContext.request().getParam("id")))
      .thenCompose(InstancesValidators::refuseWhenInstanceNotFound)
      .thenCompose(existingInstance -> fetchPrecedingSucceedingTitles(new Success<>(existingInstance), rContext, wContext))
      .thenCompose(existingInstance -> refuseWhenBlockedFieldsChanged(existingInstance, updatedInstance))
      .thenCompose(existingInstance -> refuseWhenHridChanged(existingInstance, updatedInstance))
      .thenAccept(existingInstance -> updateInstance(existingInstance, updatedInstance, rContext, wContext))
      .exceptionally(doExceptionally(rContext));
  }

  private void patch(RoutingContext rContext) {
    WebContext wContext = new WebContext(rContext);
    JsonObject patchJson = rContext.body().asJsonObject();
    String id = rContext.request().getParam("id");
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);

    completedFuture(patchJson)
      .thenCompose(patch -> instanceCollection.findById(id))
      .thenCompose(InstancesValidators::refuseWhenInstanceNotFound)
      .thenCompose(existingInstance ->
        fetchPrecedingSucceedingTitles(new Success<>(existingInstance), rContext, wContext))
      .thenCompose(existingInstance ->
        fetchInstanceRelationships(new Success<>(existingInstance), rContext, wContext))
      .thenCompose(existingInstance ->
        applyPatch(existingInstance, patchJson)
          .thenCompose(patchedInstance ->
            refuseWhenSuppressFlagsInvalid(patchedInstance)
              .thenCompose(InstancePrecedingSucceedingTitleValidators::refuseWhenUnconnectedHasNoTitle)
              .thenCompose(i -> refuseWhenBlockedFieldsChanged(existingInstance, patchedInstance))
              .thenCompose(i -> refuseWhenHridChanged(existingInstance, patchedInstance))
              .thenAccept(i -> patchInstance(existingInstance, patchedInstance, patchJson, rContext, wContext))))
      .exceptionally(doExceptionally(rContext));
  }

  private CompletableFuture<Instance> applyPatch(Instance existingInstance, JsonObject patchJson) {
    try {
      JsonObject mergedJson = JsonObject.mapFrom(existingInstance);
      mergedJson.mergeIn(patchJson, false);
      if (isInstanceControlledByRecord(existingInstance)) {
        zeroingField(mergedJson.getJsonArray(Instance.PRECEDING_TITLES_KEY), PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY);
        zeroingField(mergedJson.getJsonArray(Instance.SUCCEEDING_TITLES_KEY), PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY);
      }
      return completedFuture(Instance.fromJson(mergedJson));
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  /**
   * Call Source record storage to update suppress from discovery flag in underlying record
   *
   * @param srsClient       - SourceStorageRecordsClient
   * @param updatedInstance - Updated instance entity
   */
  private CompletableFuture<Void> updateSuppressFromDiscoveryFlag(SourceStorageRecordsClient srsClient, Instance updatedInstance) {
    try {
      return srsClient.putSourceStorageRecordsSuppressFromDiscoveryById(updatedInstance.getId(), INSTANCE_ID_TYPE, updatedInstance.getDiscoverySuppress())
        .toCompletionStage()
        .toCompletableFuture()
        .thenCompose(httpClientResponse -> {
          if (httpClientResponse.statusCode() == HttpStatus.HTTP_OK.toInt()) {
            log.info(format("Suppress from discovery flag was successfully updated for record in SRS. InstanceID: %s",
              updatedInstance.getId()));
            return CompletableFuture.completedFuture(null);
          } else {
            String errorMessage = format("Failed to update suppress from discovery flag for record in SRS. InstanceID: %s, StatusCode: %s",
              updatedInstance.getId(), httpClientResponse.statusCode());
            log.error(errorMessage);
            return CompletableFuture.failedFuture(new InternalServerErrorException(errorMessage));
          }
        });
    } catch (Exception e) {
      log.error("Error during updating suppress from discovery flag for record in SRS", e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<Void> deleteSourceStorageRecord(WebContext wContext, String instanceId) {
    try {
      SourceStorageRecordsClient srsClient = getSourceStorageRecordsClient(wContext);
      return srsClient.deleteSourceStorageRecordsById(instanceId, INSTANCE_ID_TYPE)
        .toCompletionStage()
        .toCompletableFuture()
        .thenCompose(response -> {
          if (response.statusCode() == HttpStatus.HTTP_NO_CONTENT.toInt()) {
            log.info("deleteSourceStorageRecord:: MARC record was successfully marked as deleted in SRS. instanceID: {}", instanceId);
            return CompletableFuture.completedFuture(null);
          } else {
            String errorMessage = response.statusCode() == HttpStatus.HTTP_NOT_FOUND.toInt()
              ? format("MARC record was not set for deletion because it was not found by instance ID: %s", instanceId)
              : format("Failed to set MARC record for deletion by instanceID: %s, statusCode: %s, body: %s",
                instanceId, response.statusCode(), response.bodyAsString());
            log.warn("deleteSourceStorageRecord:: {}", errorMessage);
            return CompletableFuture.failedFuture(new InternalServerErrorException(errorMessage));
          }
        });
    } catch (Exception e) {
      log.error("deleteSourceStorageRecord:: Error during source storage record deletion in SRS by instanceId: {}", instanceId, e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<Void> unDeleteSourceStorageRecord(SourceStorageRecordsClient srsClient, Instance updatedInstance) {
    var id = updatedInstance.getId();
    try {
      return srsClient.postSourceStorageRecordsUnDeleteById(id, INSTANCE_ID_TYPE)
        .toCompletionStage()
        .toCompletableFuture()
        .thenCompose(httpClientResponse -> {
        if (httpClientResponse.statusCode() == HttpStatus.HTTP_NO_CONTENT.toInt()) {
          log.info(format("The instance was successfully undeleted in SRS. InstanceID: %s", id));
          return CompletableFuture.completedFuture(null);
        } else {
          String errorMessage = format("The instance wasn't undeleted in SRS. InstanceID: %s, SC: %s", id,
            httpClientResponse.statusCode());
          log.error(errorMessage);
          return CompletableFuture.failedFuture(new InternalServerErrorException(errorMessage));
        }
      });
    } catch (Exception e) {
      log.error(format("Error during undelete operation for the instance: %s", id), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private SourceStorageRecordsClient getSourceStorageRecordsClient(WebContext wContext) {
    return new SourceStorageRecordsClient(wContext.getOkapiLocation(), wContext.getTenantId(), wContext.getToken(), client);
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
   * @param existingInstance existing instance
   * @param updatedInstance  instance for update
   * @param rContext         routing context
   * @param wContext         web context
   */
  private void updateInstance(Instance existingInstance,
                              Instance updatedInstance,
                              RoutingContext rContext,
                              WebContext wContext) {
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);
    instanceCollection.update(
      updatedInstance,
      v -> {
        updateRelatedRecords(rContext, wContext, updatedInstance)
          .thenCompose(ignored -> updateVisibilityFlagsInSrs(existingInstance, updatedInstance, wContext))
          .thenAccept(ignored -> noContent(rContext.response()))
          .exceptionally(doExceptionally(rContext));
      },
      FailureResponseConsumer.serverError(rContext.response()));
  }

  /**
   * Patches given Instance
   *
   * @param existingInstance existing instance
   * @param patchedInstance  instance for update
   * @param patchJson        patch body
   * @param rContext         routing context
   * @param wContext         web context
   */
  private void patchInstance(Instance existingInstance,
    Instance patchedInstance,
    JsonObject patchJson,
    RoutingContext rContext,
    WebContext wContext) {
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);
    instanceCollection.patch(
      existingInstance.getId(),
      patchJson,

      v -> updateRelatedRecords(rContext, wContext, patchedInstance)
        .thenCompose(ignored -> updateVisibilityFlagsInSrs(existingInstance, patchedInstance, wContext))
        .thenAccept(ignored -> noContent(rContext.response()))
        .exceptionally(doExceptionally(rContext)),
      FailureResponseConsumer.serverError(rContext.response()));
  }

  private CompletableFuture<Void> updateVisibilityFlagsInSrs(Instance existing, Instance updated, WebContext wContext) {
    if (!isInstanceControlledByRecord(updated)) {
      return CompletableFuture.completedFuture(null);
    }
    if (isTrue(updated.getDeleted()) && notEqual(existing.getDeleted(), updated.getDeleted())) {
      return deleteSourceStorageRecord(wContext, updated.getId());
    } else if (isFalse(updated.getDeleted())) {
      SourceStorageRecordsClient srsClient = getSourceStorageRecordsClient(wContext);
      CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

      if (notEqual(existing.getDiscoverySuppress(), updated.getDiscoverySuppress())) {
        future = future.thenCompose(v -> updateSuppressFromDiscoveryFlag(srsClient, updated));
      }
      if (notEqual(existing.getDeleted(), updated.getDeleted())) {
        future = future.thenCompose(v -> unDeleteSourceStorageRecord(srsClient, updated));
      }
      return future;
    }
    return CompletableFuture.completedFuture(null);
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

    // We still need zeroing "succeedingInstanceId" in precedingTitles and "precedingInstanceId" in succeedingTitles
    // because these fields are not provided for/from UI requests. We just ignore these fields on comparing as a blocked fields.
    // Anyway they are filled after this comparing while fetching from DB in "updatePrecedingSucceedingTitles"-method.
    zeroingField(existingInstanceJson.getJsonArray(Instance.PRECEDING_TITLES_KEY), PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY);
    zeroingField(existingInstanceJson.getJsonArray(Instance.SUCCEEDING_TITLES_KEY), PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY);

    Map<String, Object> existingBlockedFields = new HashMap<>();
    Map<String, Object> updatedBlockedFields = new HashMap<>();
    for (String blockedFieldCode : config.getInstanceBlockedFields()) {
      existingBlockedFields.put(blockedFieldCode, existingInstanceJson.getValue(blockedFieldCode));
      updatedBlockedFields.put(blockedFieldCode, updatedInstanceJson.getValue(blockedFieldCode));
    }
    return notEqual(existingBlockedFields, updatedBlockedFields);
  }

  private void zeroingField(JsonArray precedingSucceedingTitles, String precedingSucceedingInstanceId) {
    if (precedingSucceedingTitles.isEmpty()) {
      return;
    }
    for (int index = 0; index < precedingSucceedingTitles.size(); index++) {
      JsonObject jsonObject = precedingSucceedingTitles.getJsonObject(index);
      jsonObject.put(precedingSucceedingInstanceId, null);
    }
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).empty(
      routingContext.request().getParam("query"),
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

  private void softDelete(RoutingContext routingContext) {
    WebContext webContext = new WebContext(routingContext);
    InstanceCollection instanceCollection = storage.getInstanceCollection(webContext);
    instanceCollection.findById(routingContext.request().getParam("id"))
      .thenCompose(instance -> instance == null
        ? failedFuture(new NotFoundException("Instance not found"))
        : updateVisibility(instance, instanceCollection))
    .thenCompose(instance -> isInstanceControlledByRecord(instance)
        ? deleteSourceStorageRecord(webContext, instance.getId())
        : CompletableFuture.completedFuture(null))
      .thenAccept(v -> noContent(routingContext.response()))
      .exceptionally(doExceptionally(routingContext));
  }

  private CompletableFuture<Instance> updateVisibility(Instance instance, InstanceCollection instanceCollection) {
    instance.setDiscoverySuppress(true);
    instance.setStaffSuppress(true);
    instance.setDeleted(true);
    return instanceCollection.update(instance)
      .thenApply(v -> {
        log.info("updateVisibility:: staffSuppress, discoverySuppress and deleted properties are set to true for instance with id: '{}'",
          instance.getId());
        return instance;
      });
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
            .thenCompose(response -> setBoundWithFlag(it, routingContext, context))
            .thenAccept(response -> successResponse(routingContext, context, response));
        } else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private CompletableFuture<Instance> setBoundWithFlag (Success<Instance> success, RoutingContext routingContext, WebContext webContext) {
    Instance instance = success.getResult();
    return findBoundWithHoldingsIdsForInstanceId(instance.getId(), routingContext, webContext).thenCompose(
      boundWithHoldings -> {
        instance.setIsBoundWith(boundWithHoldings != null && !boundWithHoldings.isEmpty());
        return completedFuture(instance);
      }
    );
  }

  /**
   * Fetches instance relationships for multiple Instance records, populates, responds
   *
   * @param instancesResponse Multi record Instances result
   * @param routingContext Routing
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

  private CompletableFuture<List<JsonObject>> fetchHoldingsRecordsForInstanceRecordSet(
    InstancesResponse instancesResponse, RoutingContext routingContext, WebContext context) {
    List<String> instanceIds = getInstanceIdsFromInstanceResult(instancesResponse.getSuccess());

    MultipleRecordsFetchClient holdingsFetcher = MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName("holdingsRecords")
      .withExpectedStatus(200)
      .withCollectionResourceClient(createHoldingsStorageClient(routingContext, context))
      .build();

    return holdingsFetcher.find(instanceIds, this::cqlMatchAnyByInstanceIds);
  }

  /**
   * Retrieves a list of IDs for the holdings under the provided Instance that are part of a bound-with.
   * @param instanceId  The ID of the Instance to find bound-with holdings for
   * @param routingContext  Routing
   * @param webContext      Context
   * @return List of IDs of holdings records that are bound-with
   */
  private CompletableFuture<List<String>> findBoundWithHoldingsIdsForInstanceId(
    String instanceId, RoutingContext routingContext, WebContext webContext ) {
    CompletableFuture<Response> holdingsFuture = new CompletableFuture<>();

    createHoldingsStorageClient(routingContext, webContext).getAll("instanceId=="+instanceId, holdingsFuture::complete);
    return holdingsFuture.thenCompose(
      response -> {
        List<String> holdingsRecordsList =
          response.getJson().getJsonArray("holdingsRecords")
            .stream().map(o -> ((JsonObject) o).getString("id")).collect(Collectors.toList());
        return checkHoldingsForBoundWith(holdingsRecordsList, routingContext, webContext);
      }
    );
  }

  /**
   * From the provided list of holdings record IDs, finds out which of them
   * are part of  bound-withs -- if any -- and returns a list of those.
   * @param holdingsRecordIds holdings records to check for bound-with
   * @param routingContext Routing
   * @param webContext Context
   * @return List of IDs for holdings records that are bound with others.
   */
  private CompletableFuture<List<String>> checkHoldingsForBoundWith(
                                              List<String> holdingsRecordIds,
                                              RoutingContext routingContext,
                                              WebContext webContext) {
    List<String> holdingsRecordsThatAreBoundWith = new ArrayList<>();
    String holdingsRecordIdKey = "holdingsRecordId";
    // Check if any IDs in the list of holdings appears in bound-with-parts
    return MultipleRecordsFetchClient
      .builder()
      .withCollectionPropertyName("boundWithParts")
      .withExpectedStatus(200)
      .withCollectionResourceClient(createBoundWithPartsClient(routingContext, webContext))
      .build()
      .find(holdingsRecordIds, this::cqlMatchAnyByHoldingsRecordIds)
      .thenCompose( boundWithParts -> {
          holdingsRecordsThatAreBoundWith.addAll(boundWithParts.stream()
           .map( boundWithPart -> boundWithPart.getString( holdingsRecordIdKey ))
           .collect( Collectors.toList()));
           // Check if any of the holdings has an item that appears in bound-with-parts
           // First, find the holdings' items
           return MultipleRecordsFetchClient
             .builder()
             .withCollectionPropertyName( "items" )
             .withExpectedStatus( 200 )
             .withCollectionResourceClient( createItemsStorageClient( routingContext, webContext ) )
             .build()
             .find( holdingsRecordIds, this::cqlMatchAnyByHoldingsRecordIds)
             .thenCompose(
               items -> {
                 if (items.isEmpty()) {
                  return CompletableFuture.completedFuture(Collections.emptyList());
                 }
                 List<String> itemIds = new ArrayList<>();
                 Map<String,String> itemHoldingsMap = new HashMap<>();
                 for (JsonObject item : items) {
                   itemHoldingsMap.put(item.getString( "id" ), item.getString( holdingsRecordIdKey ));
                   itemIds.add(item.getString( "id" ));
                 }
                 // Then look up the items in bound-with-parts
                 return MultipleRecordsFetchClient
                   .builder()
                   .withCollectionPropertyName( "boundWithParts" )
                   .withExpectedStatus( 200 )
                   .withCollectionResourceClient( createBoundWithPartsClient( routingContext, webContext ) )
                   .build()
                   .find( itemIds, this::cqlMatchAnyByItemIds )
                   .thenCompose( boundWithParts2 ->
                   {
                     List<String> boundWithItemIds =
                       boundWithParts2.stream()
                         .map(boundWithPart2 -> boundWithPart2.getString( "itemId" ))
                         .distinct()
                         .toList();
                     for (String itemId : boundWithItemIds) {
                       holdingsRecordsThatAreBoundWith.add(itemHoldingsMap.get(itemId));
                     }
                     return completedFuture( holdingsRecordsThatAreBoundWith );
                   });
               });
        });
  }

  /**
   * Checks if any holdings/items under the listed instances are parts of bound-withs
   * and sets a flag on each Instance in the response where that is true
   * @param instancesResponse Instance result set
   * @param routingContext Routing
   * @param webContext Context
   * @return Returns the provided result set with 0 or more Instances marked as bound-with
   */
  private CompletableFuture<InstancesResponse> lookUpBoundWithsForInstanceRecordSet(
                                                  InstancesResponse instancesResponse,
                                                  RoutingContext routingContext,
                                                  WebContext webContext) {

    if (instancesResponse.hasRecords()) {
      return fetchHoldingsRecordsForInstanceRecordSet(instancesResponse, routingContext, webContext)
        .thenCompose(holdingsRecordList -> {
          if (holdingsRecordList.isEmpty()) {
            return completedFuture(instancesResponse);
          } else {
            Map<String, String> holdingsToInstanceMap = new HashMap<>();
            for (JsonObject holdingsRecord : holdingsRecordList) {
              holdingsToInstanceMap.put(holdingsRecord.getString(ID), holdingsRecord.getString(INSTANCE_ID));
            }
            ArrayList<String> holdingsIdsList = new ArrayList<>(holdingsToInstanceMap.keySet());
            return checkHoldingsForBoundWith(holdingsIdsList, routingContext, webContext)
              .thenCompose(holdingsRecordIds -> {
                  List<String> boundWithInstanceIds = new ArrayList<>();
                  for (String holdingsRecordId : holdingsRecordIds) {
                    boundWithInstanceIds.add(holdingsToInstanceMap.get(holdingsRecordId));
                  }
                  instancesResponse.setBoundWithInstanceIds(boundWithInstanceIds);
                return completedFuture(instancesResponse);
                }
              );
          }
        });
    } else {
      return completedFuture(instancesResponse);
    }
  }

  /**
   * Fetches instance relationships for a single Instance result, populates, responds
   *
   * @param success        Single record Instance result
   * @param routingContext Routing
   * @param context        Context
   */
  private CompletableFuture<Instance> fetchInstanceRelationships(
    Success<Instance> success, RoutingContext routingContext, WebContext context) {

    Instance instance = success.getResult();
    List<String> instanceIds = getInstanceIdsFromInstanceResult(success);
    String query = createQueryForInstanceRelationships(instanceIds);
    CollectionResourceClient relatedInstancesClient =
      createInstanceRelationshipsClient(routingContext, context);

    if (relatedInstancesClient != null) {
      CompletableFuture<Response> relatedInstancesFetched = new CompletableFuture<>();

      relatedInstancesClient.getMany(query, Integer.MAX_VALUE, 0, relatedInstancesFetched::complete);

      return relatedInstancesFetched
        .thenCompose(response -> withInstanceRelationships(instance, response));
    }
    return completedFuture(null);
  }

  private void successResponse(RoutingContext routingContext, WebContext context,
    Instance instance) {

    JsonResponse.success(routingContext.response(), instance.getJsonForResponse(context));
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

    precedingSucceedingTitlesClient.getAll(queryForPrecedingSucceedingInstances, precedingSucceedingTitlesFetched::complete);

    return precedingSucceedingTitlesFetched
      .thenCompose(response ->
        withPrecedingSucceedingTitles(routingContext, context, instance, response));
  }

  // Utilities

  private CqlQuery cqlMatchAnyByInstanceIds(List<String> instanceIds) {
    return CqlQuery.exactMatchAny(INSTANCE_ID, instanceIds);
  }

  private CqlQuery cqlMatchAnyByHoldingsRecordIds(List<String> holdingsRecordIds) {
    return CqlQuery.exactMatchAny("holdingsRecordId", holdingsRecordIds);
  }

  private CqlQuery cqlMatchAnyByItemIds (List<String> itemIds) {
    return CqlQuery.exactMatchAny( "itemId", itemIds );
  }

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
            PrecedingSucceedingTitle.PRECEDING_INSTANCE_ID_KEY)).toList();

      List<CompletableFuture<PrecedingSucceedingTitle>> succeedingTitleCompletableFutures =
        relationsList.stream().filter(rel -> isSucceedingTitle(instance, rel))
          .map(rel -> getPrecedingSucceedingTitle(routingContext, context, rel,
            PrecedingSucceedingTitle.SUCCEEDING_INSTANCE_ID_KEY)).toList();

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

  private CompletionStage<Instance> refuseWhenSuppressFlagsInvalid(Instance instance) {
    if (isTrue(instance.getDeleted())
      && (isFalse(instance.getStaffSuppress()) || isFalse(instance.getDiscoverySuppress()))) {
      log.error("refuseWhenSuppressFlagsInvalid:: Error during instance processing, cause: {}", SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE);
      return failedFuture(new BadRequestException(SUPPRESSION_FLAGS_INCONSISTENCY_MESSAGE));
    }
    return completedFuture(instance);
  }
}
