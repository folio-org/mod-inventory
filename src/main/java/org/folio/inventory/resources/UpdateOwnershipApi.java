package org.folio.inventory.resources;

import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.HoldingsUpdateOwnership;
import org.folio.NotUpdatedEntity;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Status;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

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
  public static final String INSTANCE_NOT_FOUND_AT_SOURCE_TENANT = "Instance with id: %s not found at source tenant, tenant: %s";
  public static final String TENANT_NOT_IN_CONSORTIA = "%s tenant is not in consortia";
  public static final String HOLDINGS_NOT_FOUND = "HoldingsRecord with id: %s not found on tenant: %s";
  public static final String LOG_UPDATE_HOLDINGS_OWNERSHIP = "updateHoldingsOwnership:: %s";

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
                    String instanceNotSharedErrorMessage = String.format(INSTANCE_NOT_SHARED, holdingsUpdateOwnership.getToInstanceId());
                    LOGGER.warn(String.format(LOG_UPDATE_HOLDINGS_OWNERSHIP, instanceNotSharedErrorMessage));
                    return CompletableFuture.failedFuture(new BadRequestException(instanceNotSharedErrorMessage));
                  }
                } else {
                  String instanceNotFoundErrorMessage = String.format(INSTANCE_NOT_FOUND_AT_SOURCE_TENANT, holdingsUpdateOwnership.getToInstanceId(), context.getTenantId());
                  LOGGER.warn(String.format(LOG_UPDATE_HOLDINGS_OWNERSHIP, instanceNotFoundErrorMessage));
                  return CompletableFuture.failedFuture(new NotFoundException(instanceNotFoundErrorMessage));
                }
              });
          }
          String notInConsortiaErrorMessage = String.format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn(String.format(LOG_UPDATE_HOLDINGS_OWNERSHIP, notInConsortiaErrorMessage));
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
    // should be implemented in MODINV-955
  }

  private CompletableFuture<List<String>> updateOwnershipOfHoldingsRecords(HoldingsUpdateOwnership holdingsUpdateOwnership,
                                                                           List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext,
                                                                           WebContext context, Context targetTenantContext) {
    try {
      LOGGER.info("updateOwnershipOfHoldingsRecords:: Updating ownership of holdingsRecord: {}, to tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), targetTenantContext.getTenantId());

      CollectionResourceClient holdingsStorageClient = createHoldingsStorageClient(createHttpClient(client, routingContext, context),
        context);
      MultipleRecordsFetchClient holdingsRecordFetchClient = createHoldingsRecordsFetchClient(holdingsStorageClient);

      HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);
      HoldingsRecordCollection targetTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(targetTenantContext);

      return holdingsRecordFetchClient.find(holdingsUpdateOwnership.getHoldingsRecordIds(), MoveApiUtil::fetchByIdCql)
        .thenCompose(jsons -> {
          LOGGER.info("updateOwnershipOfHoldingsRecords:: Found holdings to update ownership: {}", jsons);
          processNotFoundInstances(holdingsUpdateOwnership.getHoldingsRecordIds(), notUpdatedEntities, context, jsons);
          if (!jsons.isEmpty()) {
            return createHoldings(jsons, notUpdatedEntities, holdingsUpdateOwnership.getToInstanceId(), targetTenantHoldingsRecordCollection)
              .thenCompose(createdHoldings -> {
                LOGGER.info("updateOwnershipOfHoldingsRecords:: Created holdings: {}, for tenant: {}", createdHoldings, targetTenantContext.getTenantId());

                return transferAttachedItems(createdHoldings, notUpdatedEntities, routingContext, context, targetTenantContext)
                  .thenCompose(itemIds ->
                    deleteHoldings(getHoldingsToDelete(notUpdatedEntities, createdHoldings), notUpdatedEntities, sourceTenantHoldingsRecordCollection));
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

  private CompletableFuture<List<String>> transferAttachedItems(List<UpdateOwnershipHoldingsRecord> holdingsRecords, List<NotUpdatedEntity> notUpdatedEntities,
                                                                RoutingContext routingContext, WebContext context, Context targetTenantContext) {
    List<String> sourceHoldingsRecordsIds = new ArrayList<>();

    try {
      sourceHoldingsRecordsIds = holdingsRecords.stream().map(UpdateOwnershipHoldingsRecord::getSourceId).toList();

      LOGGER.info("transferAttachedItems:: Transfer items of holdingsRecordIds: {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId());

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsFetchClient = createItemsFetchClient(itemsStorageClient);

      ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
      ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

      return itemsFetchClient.find(sourceHoldingsRecordsIds, MoveApiUtil::fetchByHoldingsRecordIdCql)
        .thenCompose(jsons -> {
          LOGGER.info("transferAttachedItems:: Found items to transfer: {}", jsons);
          if (!jsons.isEmpty()) {
            return createItems(jsons, holdingsRecords, notUpdatedEntities, targetTenantItemCollection)
              .thenCompose(items -> deleteItems(items, notUpdatedEntities, sourceTenantItemCollection));
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("transferAttachedItems:: Error during transfer attached items for holdings {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<UpdateOwnershipItem>> createItems(List<JsonObject> jsons, List<UpdateOwnershipHoldingsRecord> holdingsRecords,
                                                                   List<NotUpdatedEntity> notUpdatedEntities, ItemCollection itemCollection) {
    List<CompletableFuture<UpdateOwnershipItem>> createFutures = jsons.stream()
      .map(itemJson -> {
        String sourceId = itemJson.getString("id");
        itemJson.remove("id");

        Item item = ItemUtil.fromStoredItemRepresentation(itemJson)
          .withHrid(null);

        String sourceHoldingId = item.getHoldingId();

        String targetHoldingId = holdingsRecords.stream()
          .filter(h -> h.getSourceId().equals(sourceHoldingId)).findFirst().map(HoldingsRecord::getId)
          .orElse(null);

        return itemCollection.add(item.withHoldingId(targetHoldingId))
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating item with id: {} for holdingsRecord with id: {}", item.getId(), item.getHoldingId(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHoldingId).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(i -> mapUpdateOwnershipItem(i).withSourceId(sourceId).withSourceHoldingId(sourceHoldingId));
      })
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<UpdateOwnershipHoldingsRecord>> createHoldings(List<JsonObject> jsons, List<NotUpdatedEntity> notUpdatedEntities, String instanceId,
                                                                                HoldingsRecordCollection holdingsRecordCollection) {
    jsons.forEach(MoveApiUtil::removeExtraRedundantFields);

    List<HoldingsRecord> holdingsRecordsToUpdateOwnership = jsons.stream()
      .map(json -> json.mapTo(HoldingsRecord.class).withHrid(null))
      .filter(holdingsRecord -> holdingsRecord.getInstanceId().equals(instanceId))
      .toList();

    List<CompletableFuture<UpdateOwnershipHoldingsRecord>> createFutures = holdingsRecordsToUpdateOwnership.stream()
      .map(holdingsRecord -> {
        String sourceId = holdingsRecord.getId();
        return holdingsRecordCollection.add(holdingsRecord.withId(null))
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating holdingsRecord with id: {}", holdingsRecord.getId(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceId).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(h -> mapUpdateOwnershipHoldingsRecord(h).withSourceId(sourceId));
      })
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteHoldings(List<UpdateOwnershipHoldingsRecord> holdingsRecords, List<NotUpdatedEntity> notUpdatedEntities,
                                                         HoldingsRecordCollection holdingsRecordCollection) {
    List<CompletableFuture<String>> deleteFutures = holdingsRecords.stream()
      .map(holdingsRecord -> {
        Promise<String> promise = Promise.promise();
        holdingsRecordCollection.delete(holdingsRecord.getSourceId(), success -> promise.complete(holdingsRecord.getSourceId()),
          failure -> {
            LOGGER.warn("deleteHoldings:: Error during deleting holdingsRecord with id: {}, status code: {}, reason: {}",
              holdingsRecord.getSourceId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecord.getSourceId()).withErrorMessage(failure.getReason()));
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

  private CompletableFuture<List<String>> deleteItems(List<UpdateOwnershipItem> items, List<NotUpdatedEntity> notUpdatedEntities, ItemCollection itemCollection) {
    List<CompletableFuture<String>> deleteFutures = items.stream()
      .map(item -> {
        Promise<String> promise = Promise.promise();
        itemCollection.delete(item.getSourceId(), success -> promise.complete(item.getSourceId()),
          failure -> {
            LOGGER.warn("deleteItems:: Error during deleting item with id: {} for holdingsRecord with id {}, status code: {}, reason: {}",
              item.getSourceId(), item.getSourceHoldingId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(item.getSourceHoldingId()).withErrorMessage(failure.getReason()));
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

  private void processNotFoundInstances(List<String> holdingsRecordIds, List<NotUpdatedEntity> notUpdatedEntities, WebContext context, List<JsonObject> jsons) {
    List<String> foundIds = jsons.stream().map(json -> json.getString("id")).toList();
    List<String> notFoundIds = ListUtils.subtract(holdingsRecordIds, foundIds);
    notFoundIds.forEach(id -> {
      String errorMessage = String.format(HOLDINGS_NOT_FOUND, id, context.getTenantId());
      LOGGER.warn(String.format("processNotFoundInstances:: %s", errorMessage));
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(id).withErrorMessage(errorMessage));
    });
  }

  private List<UpdateOwnershipHoldingsRecord> getHoldingsToDelete(List<NotUpdatedEntity> notUpdatedEntities, List<UpdateOwnershipHoldingsRecord> createdHoldings) {
    List<String> notUpdatedHoldingsIds = notUpdatedEntities.stream().map(NotUpdatedEntity::getEntityId).toList();
    return createdHoldings.stream().filter(holdingsRecord -> !notUpdatedHoldingsIds.contains(holdingsRecord.getSourceId())).toList();
  }

  private UpdateOwnershipHoldingsRecord mapUpdateOwnershipHoldingsRecord(HoldingsRecord holdingsRecord) {
    return JsonObject.mapFrom(holdingsRecord).mapTo(UpdateOwnershipHoldingsRecord.class);
  }

  private UpdateOwnershipItem mapUpdateOwnershipItem(Item item) {
    return JsonObject.mapFrom(item).mapTo(UpdateOwnershipItem.class);
  }

  @Getter
  private static class UpdateOwnershipHoldingsRecord extends HoldingsRecord {
    private String sourceId;

    public UpdateOwnershipHoldingsRecord withSourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }
  }

  @Getter
  private static class UpdateOwnershipItem extends Item {
    private String sourceId;
    private String sourceHoldingId;

    public UpdateOwnershipItem(String id, String version, String holdingId, Status status, String materialTypeId, String permanentLoanTypeId, JsonObject metadata) {
      super(id, version, holdingId, status, materialTypeId, permanentLoanTypeId, metadata);
    }

    public UpdateOwnershipItem withSourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public UpdateOwnershipItem withSourceHoldingId(String sourceHoldingId) {
      this.sourceHoldingId = sourceHoldingId;
      return this;
    }
  }
}
