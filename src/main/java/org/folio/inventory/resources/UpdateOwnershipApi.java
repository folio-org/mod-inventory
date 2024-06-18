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
import org.folio.NotUpdatedEntity;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
  public static final String INSTANCE_NOT_FOUND = "Instance with id: %s not found on tenant: %s";

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
                    LOGGER.warn("updateHoldingsOwnership:: " + instanceNotSharedErrorMessage);
                    return CompletableFuture.failedFuture(new BadRequestException(instanceNotSharedErrorMessage));
                  }
                } else {
                  String instanceNotFoundErrorMessage = String.format(INSTANCE_NOT_FOUND_AT_SOURCE_TENANT, holdingsUpdateOwnership.getToInstanceId(), context.getTenantId());
                  LOGGER.warn("updateHoldingsOwnership:: " + instanceNotFoundErrorMessage);
                  return CompletableFuture.failedFuture(new NotFoundException(instanceNotFoundErrorMessage));
                }
              });
          }
          String notInConsortiaErrorMessage = String.format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn("updateHoldingsOwnership:: " + notInConsortiaErrorMessage, context);
          return CompletableFuture.failedFuture(new BadRequestException(notInConsortiaErrorMessage));
        })
        .thenAccept(updateHoldingsRecords -> respond(routingContext, notUpdatedEntities))
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
          LOGGER.info("updateOwnershipOfHoldingsRecords:: Founded holdings to update ownership: {}", jsons);
          processNotFoundedInstances(holdingsUpdateOwnership.getHoldingsRecordIds(), notUpdatedEntities, context, jsons);
          return createHoldings(jsons, notUpdatedEntities, holdingsUpdateOwnership.getToInstanceId(), targetTenantHoldingsRecordCollection);
        })
        .thenCompose(createdHoldings -> {
          LOGGER.info("updateOwnershipOfHoldingsRecords:: Created holdings: {}, for tenant: {}", createdHoldings, targetTenantContext.getTenantId());
          List<String> createdHoldingsIds = createdHoldings.stream().map(HoldingsRecord::getId).toList();

          return transferAttachedItems(createdHoldingsIds, notUpdatedEntities, routingContext, context, targetTenantContext)
            .thenCompose(itemIds ->
              deleteHoldings(getHoldingsToDelete(notUpdatedEntities, createdHoldings), notUpdatedEntities, sourceTenantHoldingsRecordCollection));
        });
    } catch (Exception e) {
      LOGGER.warn("updateOwnershipOfHoldingsRecords:: Error during update ownership of holdings {}, to tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<String>> transferAttachedItems(List<String> holdingsRecordIds, List<NotUpdatedEntity> notUpdatedEntities,
                                                                RoutingContext routingContext, WebContext context, Context targetTenantContext) {
    try {
      LOGGER.info("transferAttachedItems:: Transfer items of holdingsRecordIds: {}, to tenant: {}",
        holdingsRecordIds, targetTenantContext.getTenantId());

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsFetchClient = createItemsFetchClient(itemsStorageClient);

      ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
      ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

      return itemsFetchClient.find(holdingsRecordIds, MoveApiUtil::fetchByHoldingsRecordIdCql)
        .thenCompose(jsons -> {
          LOGGER.info("transferAttachedItems:: Found items to transfer: {}", jsons);
          return createItems(jsons, notUpdatedEntities, targetTenantItemCollection);
        })
        .thenCompose(items -> {
          LOGGER.info("transferAttachedItems:: Created items: {}", items);
          return deleteItems(items, notUpdatedEntities, sourceTenantItemCollection);
        });
    } catch (Exception e) {
      LOGGER.warn("transferAttachedItems:: Error during transfer attached items for holdings {}, to tenant: {}",
        holdingsRecordIds, targetTenantContext.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<Item>> createItems(List<JsonObject> jsons, List<NotUpdatedEntity> notUpdatedEntities, ItemCollection itemCollection) {
    List<Item> itemRecordsToUpdateOwnership = jsons.stream()
      .map(ItemUtil::fromStoredItemRepresentation)
      .toList();

    List<CompletableFuture<Item>> createFutures = itemRecordsToUpdateOwnership.stream()
      .map(item ->
        itemCollection.add(item)
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating item with id: {} for holdingsRecord with id: {}", item.getId(), item.getHoldingId(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(item.getHoldingId()).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<HoldingsRecord>> createHoldings(List<JsonObject> jsons, List<NotUpdatedEntity> notUpdatedEntities, String instanceId,
                                                                 HoldingsRecordCollection holdingsRecordCollection) {
    List<HoldingsRecord> holdingsRecordsToUpdateOwnership = jsons.stream()
      .peek(MoveApiUtil::removeExtraRedundantFields)
      .map(json -> json.mapTo(HoldingsRecord.class))
      .filter(holdingsRecord -> holdingsRecord.getInstanceId().equals(instanceId))
      .toList();

    List<CompletableFuture<HoldingsRecord>> createFutures = holdingsRecordsToUpdateOwnership.stream()
      .map(holdingRecord ->
        holdingsRecordCollection.add(holdingRecord)
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating holdingsRecord with id: {}", holdingRecord.getId(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingRecord.getId()).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteHoldings(List<HoldingsRecord> holdingsRecords, List<NotUpdatedEntity> notUpdatedEntities,
                                                         HoldingsRecordCollection holdingsRecordCollection) {
    List<CompletableFuture<String>> deleteFutures = holdingsRecords.stream()
      .map(holdingsRecord -> {
        Promise<String> promise = Promise.promise();
        holdingsRecordCollection.delete(holdingsRecord.getId(), success -> promise.complete(holdingsRecord.getId()),
          failure -> {
            LOGGER.warn("deleteHoldings:: Error during deleting holdingsRecord with id: {}, status code: {}, reason: {}",
              holdingsRecord.getId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecord.getId()).withErrorMessage(failure.getReason()));
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

  private CompletableFuture<List<String>> deleteItems(List<Item> itemIds, List<NotUpdatedEntity> notUpdatedEntities, ItemCollection itemCollection) {
    List<CompletableFuture<String>> deleteFutures = itemIds.stream()
      .map(item -> {
        Promise<String> promise = Promise.promise();
        itemCollection.delete(item.getId(), success -> promise.complete(item.getId()),
          failure -> {
            LOGGER.warn("deleteItems:: Error during deleting item with id: {} for holdingsRecord with id {}, status code: {}, reason: {}",
              item.getId(), item.getHoldingId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(item.getHoldingId()).withErrorMessage(failure.getReason()));
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

  private void processNotFoundedInstances(List<String> holdingsRecordIds, List<NotUpdatedEntity> notUpdatedEntities, WebContext context, List<JsonObject> jsons) {
    List<String> foundedIds = jsons.stream().map(json -> json.getString("id")).toList();
    List<String> notFoundedIds = ListUtils.subtract(holdingsRecordIds, foundedIds);
    notFoundedIds.forEach(id -> {
      String errorMessage = String.format(INSTANCE_NOT_FOUND, id, context.getTenantId());
      LOGGER.warn("processNotFoundedInstances:: " + errorMessage);
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(id).withErrorMessage(errorMessage));
    });
  }

  private List<HoldingsRecord> getHoldingsToDelete(List<NotUpdatedEntity> notUpdatedEntities, List<HoldingsRecord> createdHoldings) {
    List<String> notUpdatedHoldingsIds = notUpdatedEntities.stream().map(NotUpdatedEntity::getEntityId).toList();
    return createdHoldings.stream().filter(holdingsRecord -> !notUpdatedHoldingsIds.contains(holdingsRecord.getId())).toList();
  }
}
