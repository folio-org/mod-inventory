package org.folio.inventory.resources;

import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.HoldingsUpdateOwnership;
import org.folio.ItemsUpdateOwnership;
import org.folio.NotUpdatedEntity;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.MoveApiUtil;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsRecordsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createHttpClient;
import static org.folio.inventory.support.MoveApiUtil.createItemStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createItemsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.respond;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.UpdateOwnershipValidator.updateOwnershipHasRequiredFields;

public class UpdateOwnershipApi extends AbstractInventoryResource {
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  public static final String INSTANCE_NOT_SHARED = "Instance with id: %s is not shared";
  public static final String INSTANCE_RELATED_TO_HOLDINGS_RECORD_NOT_SHARED = "Instance with id: %s related to holdings record with id: %s is not shared";
  public static final String INSTANCE_NOT_FOUND_AT_SOURCE_TENANT = "Instance with id: %s not found at source tenant, tenant: %s";
  public static final String TENANT_NOT_IN_CONSORTIA = "%s tenant is not in consortia";
  public static final String HOLDINGS_RECORD_NOT_FOUND = "HoldingsRecord with id: %s not found on tenant: %s";
  public static final String ITEM_NOT_FOUND = "Item with id: %s not found on tenant: %s";
  public static final String LOG_UPDATE_HOLDINGS_OWNERSHIP = "updateHoldingsOwnership:: %s";
  public static final String LOG_UPDATE_ITEMS_OWNERSHIP = "updateItemsOwnership:: %s";
  public static final String ITEM_NOT_LINKED_TO_SHARED_INSTANCE = "Item with id: %s not linked to shared Instance";
  public static final String HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE = "HoldingsRecord with id: %s not linked to shared Instance";
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String INSTANCE_ID = "instanceId";

  private final ConsortiumService consortiumService;

  public UpdateOwnershipApi(Storage storage, HttpClient client, ConsortiumService consortiumService) {
    super(storage, client);
    this.consortiumService = consortiumService;
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/update-ownership")
      .handler(this::processUpdateItemsOwnership);
    router.post("/inventory/holdings/update-ownership")
      .handler(this::processUpdateHoldingsOwnership);
  }

  private void processUpdateHoldingsOwnership(RoutingContext routingContext) {
    try {
      final var context = new WebContext(routingContext);
      final var updateOwnershipRequest = routingContext.body().asJsonObject();

      final var validationError = updateOwnershipHasRequiredFields(context.getTenantId(), updateOwnershipRequest, HoldingsUpdateOwnership.class);

      if (validationError.isPresent()) {
        unprocessableEntity(routingContext.response(), validationError.get());
        return;
      }
      var holdingsUpdateOwnership = updateOwnershipRequest.mapTo(HoldingsUpdateOwnership.class);

      List<NotUpdatedEntity> notUpdatedEntities = new ArrayList<>();

      LOGGER.info("updateHoldingsOwnership:: Started updating ownership of holdings record: {}, to tenant: {}", holdingsUpdateOwnership.getHoldingsRecordIds(),
        holdingsUpdateOwnership.getTargetTenantId());

      consortiumService.getConsortiumConfiguration(context).toCompletionStage().toCompletableFuture()
        .thenCompose(consortiumConfigurationOptional -> {
          if (consortiumConfigurationOptional.isPresent()) {
            return storage.getInstanceCollection(context)
              .findById(holdingsUpdateOwnership.getToInstanceId())
              .thenCompose(instance -> {
                if (instance != null) {
                  if (instance.getSource().equals(CONSORTIUM_MARC.getValue()) || instance.getSource().equals(CONSORTIUM_FOLIO.getValue())) {
                    Context targetTenantContext = constructContext(holdingsUpdateOwnership.getTargetTenantId(), context.getToken(), context.getOkapiLocation());
                    return updateOwnershipOfHoldingsRecords(holdingsUpdateOwnership, notUpdatedEntities, routingContext, context, targetTenantContext);
                  } else {
                    String instanceNotSharedErrorMessage = format(INSTANCE_NOT_SHARED, holdingsUpdateOwnership.getToInstanceId());
                    LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, instanceNotSharedErrorMessage));
                    return CompletableFuture.failedFuture(new BadRequestException(instanceNotSharedErrorMessage));
                  }
                } else {
                  String instanceNotFoundErrorMessage = format(INSTANCE_NOT_FOUND_AT_SOURCE_TENANT, holdingsUpdateOwnership.getToInstanceId(), context.getTenantId());
                  LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, instanceNotFoundErrorMessage));
                  return CompletableFuture.failedFuture(new NotFoundException(instanceNotFoundErrorMessage));
                }
              });
          }
          String notInConsortiaErrorMessage = format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, notInConsortiaErrorMessage));
          return CompletableFuture.failedFuture(new BadRequestException(notInConsortiaErrorMessage));
        })
        .thenAccept(v -> respond(routingContext, notUpdatedEntities))
        .exceptionally(throwable -> {
          LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of holdings {}, to tenant: {}",
            holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId(), throwable);
          handleFailure(throwable, routingContext);
          return null;
        });
    } catch (Exception e) {
      LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of holdings", e);
      handleFailure(e, routingContext);
    }
  }

  private void processUpdateItemsOwnership(RoutingContext routingContext) {
    try {
      final var context = new WebContext(routingContext);
      final var updateOwnershipRequest = routingContext.body().asJsonObject();

      final var validationError = updateOwnershipHasRequiredFields(context.getTenantId(), updateOwnershipRequest, ItemsUpdateOwnership.class);

      if (validationError.isPresent()) {
        unprocessableEntity(routingContext.response(), validationError.get());
        return;
      }
      var itemsUpdateOwnership = updateOwnershipRequest.mapTo(ItemsUpdateOwnership.class);

      List<NotUpdatedEntity> notUpdatedEntities = new ArrayList<>();

      LOGGER.info("updateHoldingsOwnership:: Started updating ownership of item record: {}, to tenant: {}", itemsUpdateOwnership.getItemIds(),
        itemsUpdateOwnership.getTargetTenantId());

      consortiumService.getConsortiumConfiguration(context).toCompletionStage().toCompletableFuture()
        .thenCompose(consortiumConfigurationOptional -> {
          if (consortiumConfigurationOptional.isPresent()) {
            Context targetTenantContext = constructContext(itemsUpdateOwnership.getTargetTenantId(), context.getToken(), context.getOkapiLocation());
            return storage.getHoldingsRecordCollection(targetTenantContext)
              .findById(itemsUpdateOwnership.getToHoldingsRecordId())
              .thenCompose(holdingsRecord -> {
                if (holdingsRecord != null) {
                  return storage.getInstanceCollection(targetTenantContext)
                    .findById(holdingsRecord.getInstanceId())
                    .thenCompose(instance -> {
                      if (instance.getSource().equals(CONSORTIUM_MARC.getValue()) || instance.getSource().equals(CONSORTIUM_FOLIO.getValue())) {
                        return updateOwnershipOfItems(itemsUpdateOwnership, holdingsRecord, notUpdatedEntities, routingContext, context, targetTenantContext);
                      } else {
                        String instanceNotSharedErrorMessage = format(INSTANCE_RELATED_TO_HOLDINGS_RECORD_NOT_SHARED,
                          instance.getId(), itemsUpdateOwnership.getToHoldingsRecordId());

                        LOGGER.warn(format(LOG_UPDATE_ITEMS_OWNERSHIP, instanceNotSharedErrorMessage));
                        return CompletableFuture.failedFuture(new BadRequestException(instanceNotSharedErrorMessage));
                      }
                    });
                } else {
                  String holdingsRecordNotFoundErrorMessage = format(HOLDINGS_RECORD_NOT_FOUND,
                    itemsUpdateOwnership.getToHoldingsRecordId(), targetTenantContext.getTenantId());
                  LOGGER.warn(format(LOG_UPDATE_ITEMS_OWNERSHIP, holdingsRecordNotFoundErrorMessage));
                  return CompletableFuture.failedFuture(new NotFoundException(holdingsRecordNotFoundErrorMessage));
                }
              });
          }
          String notInConsortiaErrorMessage = format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, notInConsortiaErrorMessage));
          return CompletableFuture.failedFuture(new BadRequestException(notInConsortiaErrorMessage));
        })
        .thenAccept(v -> respond(routingContext, notUpdatedEntities))
        .exceptionally(throwable -> {
          LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of items {}, to tenant: {}",
            itemsUpdateOwnership.getItemIds(), itemsUpdateOwnership.getTargetTenantId(), throwable);
          handleFailure(throwable, routingContext);
          return null;
        });

    } catch (Exception e) {
      LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of items", e);
      handleFailure(e, routingContext);
    }
  }

  private CompletableFuture<List<String>> updateOwnershipOfItems(ItemsUpdateOwnership itemsUpdateOwnership, HoldingsRecord toHoldingsRecord,
                                                                 List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext, WebContext context,
                                                                 Context targetTenantContext) {
    try {
      LOGGER.debug("updateOwnershipOfHoldingsRecords:: Updating ownership of items: {}, to tenant: {}",
        itemsUpdateOwnership.getItemIds(), targetTenantContext.getTenantId());

      String sharedInstanceId = toHoldingsRecord.getInstanceId();

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsRecordFetchClient = createItemsFetchClient(itemsStorageClient);

      HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);

      return itemsRecordFetchClient.find(itemsUpdateOwnership.getItemIds(), MoveApiUtil::fetchByIdCql)
        .thenCompose(jsons -> {
          LOGGER.debug("updateOwnershipOfHoldingsRecords:: Found items to update ownership: {}", jsons);
          processNotFoundEntities(itemsUpdateOwnership.getItemIds(), notUpdatedEntities, context, jsons, ITEM_NOT_FOUND);
          if (!jsons.isEmpty()) {
            return getHoldingsByInstanceId(sourceTenantHoldingsRecordCollection, sharedInstanceId)
              .thenCompose(holdingsRecords -> {
                ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
                ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

                List<String> holdingsRecordsIds = holdingsRecords.stream().map(HoldingsRecord::getId).toList();
                List<JsonObject> validatedItems = validateItems(jsons, holdingsRecordsIds, notUpdatedEntities);

                List<UpdateOwnershipItemWrapper> itemWrappers = validatedItems.stream()
                  .map(itemJson -> mapItemWrapper(itemJson, toHoldingsRecord.getId())).toList();

                return createItems(itemWrappers, notUpdatedEntities, UpdateOwnershipItemWrapper::sourceItemId, targetTenantItemCollection)
                  .thenCompose(items -> deleteSourceItems(items, notUpdatedEntities, UpdateOwnershipItemWrapper::sourceItemId, sourceTenantItemCollection));
              });
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("updateOwnershipOfHoldingsRecords:: Error during update ownership of items {}, to tenant: {}",
        itemsUpdateOwnership.getItemIds(), itemsUpdateOwnership.getTargetTenantId(), e);

      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<String>> updateOwnershipOfHoldingsRecords(HoldingsUpdateOwnership holdingsUpdateOwnership,
                                                                           List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext,
                                                                           WebContext context, Context targetTenantContext) {
    try {
      LOGGER.debug("updateOwnershipOfHoldingsRecords:: Updating ownership of holdingsRecord: {}, to tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), targetTenantContext.getTenantId());

      CollectionResourceClient holdingsStorageClient = createHoldingsStorageClient(createHttpClient(client, routingContext, context),
        context);
      MultipleRecordsFetchClient holdingsRecordFetchClient = createHoldingsRecordsFetchClient(holdingsStorageClient);

      HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);
      HoldingsRecordCollection targetTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(targetTenantContext);

      return holdingsRecordFetchClient.find(holdingsUpdateOwnership.getHoldingsRecordIds(), MoveApiUtil::fetchByIdCql)
        .thenCompose(jsons -> {
          LOGGER.debug("updateOwnershipOfHoldingsRecords:: Found holdings to update ownership: {}", jsons);
          processNotFoundEntities(holdingsUpdateOwnership.getHoldingsRecordIds(), notUpdatedEntities, context, jsons, HOLDINGS_RECORD_NOT_FOUND);
          if (!jsons.isEmpty()) {
            List<JsonObject> validatedHoldingsRecords = validateHoldingsRecords(jsons, holdingsUpdateOwnership.getToInstanceId(), notUpdatedEntities);
            List<UpdateOwnershipHoldingsRecordWrapper> holdingsRecordWrappers =
              validatedHoldingsRecords.stream().map(this::mapHoldingsRecordWrapper).toList();

            return createHoldings(holdingsRecordWrappers, notUpdatedEntities, targetTenantHoldingsRecordCollection)
              .thenCompose(createdHoldings -> {
                LOGGER.debug("updateOwnershipOfHoldingsRecords:: Created holdings: {}, for tenant: {}",
                  createdHoldings, targetTenantContext.getTenantId());

                return transferAttachedItems(createdHoldings, notUpdatedEntities, routingContext, context, targetTenantContext)
                  .thenCompose(itemIds ->
                    deleteSourceHoldings(getHoldingsToDelete(notUpdatedEntities, createdHoldings), notUpdatedEntities, sourceTenantHoldingsRecordCollection));
              });
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("updateOwnershipOfHoldingsRecords:: Error during update ownership of holdings {}, to tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<String>> transferAttachedItems(List<UpdateOwnershipHoldingsRecordWrapper> holdingsRecordsWrappers, List<NotUpdatedEntity> notUpdatedEntities,
                                                                RoutingContext routingContext, WebContext context, Context targetTenantContext) {
    List<String> sourceHoldingsRecordsIds = new ArrayList<>();

    try {
      sourceHoldingsRecordsIds = holdingsRecordsWrappers.stream().map(UpdateOwnershipHoldingsRecordWrapper::sourceHoldingsRecordId).toList();

      LOGGER.debug("transferAttachedItems:: Transfer items of holdingsRecordIds: {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId());

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsFetchClient = createItemsFetchClient(itemsStorageClient);

      ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
      ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

      return itemsFetchClient.find(sourceHoldingsRecordsIds, MoveApiUtil::fetchByHoldingsRecordIdCql)
        .thenCompose(jsons -> {
          LOGGER.debug("transferAttachedItems:: Found items to transfer: {}", jsons);
          if (!jsons.isEmpty()) {
            List<UpdateOwnershipItemWrapper> itemWrappers = jsons.stream().map(itemJson -> {
              String targetHoldingId = getTargetHoldingId(itemJson, holdingsRecordsWrappers);
              return mapItemWrapper(itemJson, targetHoldingId);
            }).toList();

            return createItems(itemWrappers, notUpdatedEntities, UpdateOwnershipItemWrapper::sourceHoldingsRecordId, targetTenantItemCollection)
              .thenCompose(items -> deleteSourceItems(items, notUpdatedEntities, UpdateOwnershipItemWrapper::sourceHoldingsRecordId, sourceTenantItemCollection));
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("transferAttachedItems:: Error during transfer attached items for holdings {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<UpdateOwnershipItemWrapper>> createItems(List<UpdateOwnershipItemWrapper> itemWrappers, List<NotUpdatedEntity> notUpdatedEntities,
                                                                          Function<UpdateOwnershipItemWrapper, String> getEntityIdForError, ItemCollection itemCollection) {
    LOGGER.debug("createItems:: Creating items: {}", itemWrappers.stream().map(UpdateOwnershipItemWrapper::item).toList());

    List<CompletableFuture<UpdateOwnershipItemWrapper>> createFutures = itemWrappers.stream()
      .map(itemWrapper ->
        itemCollection.add(itemWrapper.item())
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating item with id: {} for holdingsRecord with id: {}",
              itemWrapper.item().getId(), itemWrapper.item().getHoldingId(), e);

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(getEntityIdForError.apply(itemWrapper)).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(i -> itemWrapper))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private UpdateOwnershipHoldingsRecordWrapper mapHoldingsRecordWrapper(JsonObject holdingsRecordJson) {
    MoveApiUtil.removeExtraRedundantFields(holdingsRecordJson);

    HoldingsRecord holdingsRecord = holdingsRecordJson.mapTo(HoldingsRecord.class).withHrid(null);
    String sourceHoldingsRecordId = holdingsRecord.getId();

    return new UpdateOwnershipHoldingsRecordWrapper(sourceHoldingsRecordId, holdingsRecord.withId(UUID.randomUUID().toString()));
  }

  private CompletableFuture<List<UpdateOwnershipHoldingsRecordWrapper>> createHoldings(List<UpdateOwnershipHoldingsRecordWrapper> holdingsRecordWrappers,
                                                                                       List<NotUpdatedEntity> notUpdatedEntities,
                                                                                       HoldingsRecordCollection holdingsRecordCollection) {
    LOGGER.debug("createHoldings:: Creating holdings records: {}",
      holdingsRecordWrappers.stream().map(UpdateOwnershipHoldingsRecordWrapper::holdingsRecord).toList());

    List<CompletableFuture<UpdateOwnershipHoldingsRecordWrapper>> createFutures = holdingsRecordWrappers.stream()
      .map(holdingsRecordWrapper ->
        holdingsRecordCollection.add(holdingsRecordWrapper.holdingsRecord())
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating holdingsRecord with id: {}", holdingsRecordWrapper.holdingsRecord(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecordWrapper.sourceHoldingsRecordId()).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(h -> holdingsRecordWrapper))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteSourceHoldings(List<UpdateOwnershipHoldingsRecordWrapper> holdingsRecordWrappers, List<NotUpdatedEntity> notUpdatedEntities,
                                                               HoldingsRecordCollection holdingsRecordCollection) {
    LOGGER.debug("deleteSourceHoldings:: Deleting holdings record with ids {}",
      holdingsRecordWrappers.stream().map(UpdateOwnershipHoldingsRecordWrapper::sourceHoldingsRecordId).toList());

    List<CompletableFuture<String>> deleteFutures = holdingsRecordWrappers.stream()
      .map(holdingsRecordWrapper -> {
        Promise<String> promise = Promise.promise();
        holdingsRecordCollection.delete(holdingsRecordWrapper.sourceHoldingsRecordId(), success -> promise.complete(holdingsRecordWrapper.sourceHoldingsRecordId()),
          failure -> {
            LOGGER.warn("deleteSourceHoldings:: Error during deleting holdingsRecord with id: {}, status code: {}, reason: {}",
              holdingsRecordWrapper.sourceHoldingsRecordId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecordWrapper.sourceHoldingsRecordId()).withErrorMessage(failure.getReason()));
            promise.fail(failure.getReason());
          });
        return promise.future().toCompletionStage().toCompletableFuture();
      }).toList();

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> deleteFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteSourceItems(List<UpdateOwnershipItemWrapper> itemWrappers, List<NotUpdatedEntity> notUpdatedEntities,
                                                            Function<UpdateOwnershipItemWrapper, String> getEntityIdForError, ItemCollection itemCollection) {
    LOGGER.debug("deleteSourceItems:: Deleting items with ids {}",
      itemWrappers.stream().map(UpdateOwnershipItemWrapper::sourceItemId).toList());

    List<CompletableFuture<String>> deleteFutures = itemWrappers.stream()
      .map(itemWrapper -> {
        Promise<String> promise = Promise.promise();
        itemCollection.delete(itemWrapper.sourceItemId(), success -> promise.complete(itemWrapper.sourceItemId()),
          failure -> {
            LOGGER.warn("deleteSourceItems:: Error during deleting item with id: {} for holdingsRecord with id {}, status code: {}, reason: {}",
              itemWrapper.sourceItemId(), itemWrapper.sourceHoldingsRecordId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(getEntityIdForError.apply(itemWrapper)).withErrorMessage(failure.getReason()));
            promise.fail(failure.getReason());
          });
        return promise.future().toCompletionStage().toCompletableFuture();
      }).toList();

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> deleteFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private void processNotFoundEntities(List<String> holdingsRecordIds, List<NotUpdatedEntity> notUpdatedEntities,
                                       WebContext context, List<JsonObject> jsons, String message) {
    List<String> foundIds = jsons.stream().map(json -> json.getString("id")).toList();
    List<String> notFoundIds = ListUtils.subtract(holdingsRecordIds, foundIds);
    notFoundIds.forEach(id -> {
      String errorMessage = format(message, id, context.getTenantId());
      LOGGER.warn(format("processNotFoundInstances:: %s", errorMessage));
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(id).withErrorMessage(errorMessage));
    });
  }

  private CompletableFuture<List<HoldingsRecord>> getHoldingsByInstanceId(HoldingsRecordCollection holdingsRecordCollection,
                                                                          String instanceId) {
    Promise<List<HoldingsRecord>> promise = Promise.promise();
    try {
      holdingsRecordCollection.findByCql(format("instanceId=%s", instanceId), PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult() == null) {
            promise.complete(new ArrayList<>());
          }
          promise.complete(findResult.getResult().records);
        },
        failure -> {
          String msg = format("Error loading inventory holdings by shared instance, instanceId: '%s' statusCode: '%s', message: '%s'",
            instanceId, failure.getStatusCode(), failure.getReason());
          LOGGER.warn("getHoldingsByInstanceId:: {}", msg);
          promise.fail(msg);
        });
    } catch (Exception e) {
      String msg = format("Error loading inventory holdings by shared instance, instanceId: '%s'", instanceId);
      LOGGER.warn("getHoldingsByInstanceId:: {}", msg, e);
      promise.fail(msg);
    }
    return promise.future().toCompletionStage().toCompletableFuture();
  }

  private List<JsonObject> validateHoldingsRecords(List<JsonObject> jsons, String toInstanceId, List<NotUpdatedEntity> notUpdatedEntities) {
    return jsons.stream().filter(holdingsRecordJson -> {
      String instanceId = holdingsRecordJson.getString(INSTANCE_ID);
      if (toInstanceId.equals(instanceId)) {
        return true;
      }
      String holdingsRecordId = holdingsRecordJson.getString("id");
      String errorMessage = format(HOLDINGS_RECORD_NOT_LINKED_TO_SHARED_INSTANCE, holdingsRecordId);
      LOGGER.warn(format("validateHoldingsRecords:: %s", errorMessage));
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecordId).withErrorMessage(errorMessage));

      return false;
    }).toList();
  }

  private List<JsonObject> validateItems(List<JsonObject> jsons, List<String> holdingsRecordsIds,
                                         List<NotUpdatedEntity> notUpdatedEntities) {
    return jsons.stream().filter(json -> {
      String holdingId = json.getString(HOLDINGS_RECORD_ID);
      if (holdingId != null && holdingsRecordsIds.contains(holdingId)) {
        return true;
      }
      String itemId = json.getString("id");
      String errorMessage = format(ITEM_NOT_LINKED_TO_SHARED_INSTANCE, itemId);
      LOGGER.warn(format("validateItems:: %s", errorMessage));
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(itemId).withErrorMessage(errorMessage));

      return false;
    }).toList();
  }

  private String getTargetHoldingId(JsonObject itemJson, List<UpdateOwnershipHoldingsRecordWrapper> holdingsRecordsWrappers) {
    return holdingsRecordsWrappers.stream()
      .filter(h -> h.sourceHoldingsRecordId().equals(itemJson.getString(HOLDINGS_RECORD_ID))).findFirst().map(wrapper -> wrapper.holdingsRecord().getId())
      .orElse(null);
  }

  private UpdateOwnershipItemWrapper mapItemWrapper(JsonObject itemJson, String targetHoldingId) {
    String sourceItemId = itemJson.getString("id");
    String sourceHoldingId = itemJson.getString(HOLDINGS_RECORD_ID);

    itemJson.put("id", UUID.randomUUID().toString());

    Item item = ItemUtil.fromStoredItemRepresentation(itemJson)
      .withHrid(null)
      .withHoldingId(targetHoldingId);

    return new UpdateOwnershipItemWrapper(sourceItemId, sourceHoldingId, item);
  }

  private List<UpdateOwnershipHoldingsRecordWrapper> getHoldingsToDelete(List<NotUpdatedEntity> notUpdatedEntities,
                                                                         List<UpdateOwnershipHoldingsRecordWrapper> createdHoldingsWrappers) {
    List<String> notUpdatedHoldingsIds = notUpdatedEntities.stream().map(NotUpdatedEntity::getEntityId).toList();
    return createdHoldingsWrappers.stream().filter(holdingsRecord -> !notUpdatedHoldingsIds.contains(holdingsRecord.sourceHoldingsRecordId())).toList();
  }

  private record UpdateOwnershipHoldingsRecordWrapper(String sourceHoldingsRecordId,
                                                      HoldingsRecord holdingsRecord) {
  }

  private record UpdateOwnershipItemWrapper(String sourceItemId,
                                            String sourceHoldingsRecordId,
                                            Item item) {
  }
}
