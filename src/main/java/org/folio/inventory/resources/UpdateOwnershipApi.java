package org.folio.inventory.resources;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.resources.Holdings.MARC_SOURCE_ID;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.MoveApiUtil.createBoundWithPartsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.createBoundWithPartsStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createHttpClient;
import static org.folio.inventory.support.MoveApiUtil.createItemStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createItemsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.respond;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.UpdateOwnershipValidator.updateOwnershipHasRequiredFields;

import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.HoldingsUpdateOwnership;
import org.folio.HttpStatus;
import org.folio.ItemsUpdateOwnership;
import org.folio.NotUpdatedEntity;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.MoveApiUtil;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

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
  public static final String HOLDING_BOUND_WITH_PARTS_ERROR = "Ownership of holdings record with linked bound with parts cannot be updated, holdings record id: %s";
  public static final String ITEM_WITH_PARTS_ERROR = "Ownership of bound with parts item cannot be updated, item id: %s";
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String ITEM_ID = "itemId";
  private static final String INSTANCE_ID = "instanceId";

  private final ConsortiumService consortiumService;
  private final SnapshotService snapshotService;
  private final InventoryClientFactory clientFactory;

  public UpdateOwnershipApi(Storage storage, HttpClient client, ConsortiumService consortiumService,
                            SnapshotService snapshotService, InventoryClientFactory clientFactory) {
    super(storage, client);
    this.consortiumService = consortiumService;
    this.snapshotService = snapshotService;
    this.clientFactory = clientFactory;
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

      validateUpdateOwnershipRequest(routingContext, context, updateOwnershipRequest, HoldingsUpdateOwnership.class);

      var holdingsUpdateOwnership = updateOwnershipRequest.mapTo(HoldingsUpdateOwnership.class);
      List<NotUpdatedEntity> notUpdatedEntities = new ArrayList<>();

      LOGGER.info("updateHoldingsOwnership:: Started updating ownership of holdings record: {}, to tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId());

      processConsortiumConfiguration(context, holdingsUpdateOwnership, notUpdatedEntities, routingContext);
    } catch (Exception e) {
      LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of holdings", e);
      handleFailure(e, routingContext);
    }
  }

  private void processConsortiumConfiguration(WebContext context,
                                              HoldingsUpdateOwnership holdingsUpdateOwnership,
                                              List<NotUpdatedEntity> notUpdatedEntities,
                                              RoutingContext routingContext) {
    consortiumService.getConsortiumConfiguration(context)
      .toCompletionStage().toCompletableFuture()
      .thenCompose(consortiumConfig -> {
        if (consortiumConfig.isPresent()) {
          return handleInstanceOwnershipUpdate(context, holdingsUpdateOwnership, notUpdatedEntities, routingContext);
        } else {
          String errorMsg = format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, errorMsg));
          return CompletableFuture.failedFuture(new BadRequestException(errorMsg));
        }
      })
      .thenAccept(v -> respond(routingContext, notUpdatedEntities))
      .exceptionally(throwable -> handleException(throwable, "Holdings", holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId(), routingContext));
  }

  private CompletableFuture<List<String>> handleInstanceOwnershipUpdate(WebContext context,
                                                                        HoldingsUpdateOwnership holdingsUpdateOwnership,
                                                                        List<NotUpdatedEntity> notUpdatedEntities,
                                                                        RoutingContext routingContext) {
    return storage.getInstanceCollection(context)
      .findById(holdingsUpdateOwnership.getToInstanceId())
      .thenCompose(instance -> {
        if (instance == null) {
          String errorMsg = format(INSTANCE_NOT_FOUND_AT_SOURCE_TENANT, holdingsUpdateOwnership.getToInstanceId(), context.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, errorMsg));
          return CompletableFuture.failedFuture(new NotFoundException(errorMsg));
        }
        if (isInstanceShared(instance)) {
          Context targetTenantContext = constructContext(holdingsUpdateOwnership.getTargetTenantId(), context.getToken(), context.getOkapiLocation(), context.getUserId(), context.getRequestId());
          return updateOwnershipOfHoldingsRecords(holdingsUpdateOwnership, notUpdatedEntities, routingContext, context, targetTenantContext);
        } else {
          String errorMsg = format(INSTANCE_NOT_SHARED, holdingsUpdateOwnership.getToInstanceId());
          LOGGER.warn(format(LOG_UPDATE_HOLDINGS_OWNERSHIP, errorMsg));
          return CompletableFuture.failedFuture(new BadRequestException(errorMsg));
        }
      });
  }

  private boolean isInstanceShared(Instance instance) {
    return instance.getSource().equals(CONSORTIUM_MARC.getValue()) || instance.getSource().equals(CONSORTIUM_FOLIO.getValue());
  }

  private void processUpdateItemsOwnership(RoutingContext routingContext) {
    try {
      final var context = new WebContext(routingContext);
      final var updateOwnershipRequest = routingContext.body().asJsonObject();

      validateUpdateOwnershipRequest(routingContext, context, updateOwnershipRequest, ItemsUpdateOwnership.class);

      var itemsUpdateOwnership = updateOwnershipRequest.mapTo(ItemsUpdateOwnership.class);
      List<NotUpdatedEntity> notUpdatedEntities = new ArrayList<>();

      LOGGER.info("updateItemsOwnership:: Started updating ownership of item record: {}, to tenant: {}",
        itemsUpdateOwnership.getItemIds(), itemsUpdateOwnership.getTargetTenantId());

      processConsortiumConfigurationForItems(context, itemsUpdateOwnership, notUpdatedEntities, routingContext);
    } catch (Exception e) {
      LOGGER.warn("updateItemsOwnership:: Error during update ownership of items", e);
      handleFailure(e, routingContext);
    }
  }

  private <T> void validateUpdateOwnershipRequest(RoutingContext context,
                                                  WebContext webContext,
                                                  JsonObject updateOwnershipRequest,
                                                  Class<T> ownershipClass) {
    final var validationError = updateOwnershipHasRequiredFields(webContext.getTenantId(), updateOwnershipRequest, ownershipClass);
    validationError.ifPresent(error -> unprocessableEntity(context.response(), error));
  }

  private void processConsortiumConfigurationForItems(WebContext context, ItemsUpdateOwnership itemsUpdateOwnership,
                                                      List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext) {
    consortiumService.getConsortiumConfiguration(context)
      .toCompletionStage().toCompletableFuture()
      .thenCompose(consortiumConfig -> {
        if (consortiumConfig.isPresent()) {
          Context targetTenantContext = constructContext(itemsUpdateOwnership.getTargetTenantId(), context.getToken(), context.getOkapiLocation(), context.getUserId(), context.getRequestId());
          return processHoldingsRecord(targetTenantContext, itemsUpdateOwnership, notUpdatedEntities, routingContext, context);
        } else {
          String errorMsg = format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_ITEMS_OWNERSHIP, errorMsg));
          return CompletableFuture.failedFuture(new BadRequestException(errorMsg));
        }
      })
      .thenAccept(v -> respond(routingContext, notUpdatedEntities))
      .exceptionally(throwable -> handleException(throwable, "Items", itemsUpdateOwnership.getItemIds(), itemsUpdateOwnership.getTargetTenantId(), routingContext));
  }

  private Void handleException(Throwable throwable, String entityName, List<String> entityIds, String targetTenantId, RoutingContext routingContext) {
    LOGGER.warn("updateOwnership:: Error during update ownership of {} {}, to tenant: {}",
      entityName, entityIds, targetTenantId, throwable);
    handleFailure(throwable, routingContext);
    return null;
  }

  private CompletableFuture<List<String>> processHoldingsRecord(Context targetTenantContext,
                                                                ItemsUpdateOwnership itemsUpdateOwnership,
                                                                List<NotUpdatedEntity> notUpdatedEntities,
                                                                RoutingContext routingContext,
                                                                WebContext context) {
    return storage.getHoldingsRecordCollection(targetTenantContext)
      .findById(itemsUpdateOwnership.getToHoldingsRecordId())
      .thenCompose(holdingsRecord -> {
        if (holdingsRecord != null) {
          return verifyLinkedInstanceAndUpdateOwnership(routingContext, holdingsRecord, targetTenantContext, itemsUpdateOwnership, notUpdatedEntities, context);
        } else {
          String errorMsg = format(HOLDINGS_RECORD_NOT_FOUND, itemsUpdateOwnership.getToHoldingsRecordId(), targetTenantContext.getTenantId());
          LOGGER.warn(format(LOG_UPDATE_ITEMS_OWNERSHIP, errorMsg));
          return CompletableFuture.failedFuture(new NotFoundException(errorMsg));
        }
      });
  }

  private CompletableFuture<List<String>> verifyLinkedInstanceAndUpdateOwnership(RoutingContext routingContext, HoldingsRecord holdingsRecord, Context targetTenantContext, ItemsUpdateOwnership itemsUpdateOwnership, List<NotUpdatedEntity> notUpdatedEntities, WebContext context) {
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
  }

  private CompletableFuture<List<String>> updateOwnershipOfItems(ItemsUpdateOwnership itemsUpdateOwnership, HoldingsRecord toHoldingsRecord,
                                                                 List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext, WebContext context,
                                                                 Context targetTenantContext) {
    try {
      LOGGER.debug("updateOwnershipOfItems:: Updating ownership of items: {}, to tenant: {}",
        itemsUpdateOwnership.getItemIds(), targetTenantContext.getTenantId());

      String sharedInstanceId = toHoldingsRecord.getInstanceId();

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsRecordFetchClient = createItemsFetchClient(itemsStorageClient);

      HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);

      return itemsRecordFetchClient.find(itemsUpdateOwnership.getItemIds(), MoveApiUtil::fetchByIdCql)
        .thenCompose(jsons -> {
          LOGGER.debug("updateOwnershipOfItems:: Found items to update ownership: {}", jsons);
          processNotFoundEntities(itemsUpdateOwnership.getItemIds(), notUpdatedEntities, context, jsons, ITEM_NOT_FOUND);
          if (!jsons.isEmpty()) {
            return getHoldingsByInstanceId(sourceTenantHoldingsRecordCollection, sharedInstanceId)
              .thenCompose(holdingsRecords -> {
                ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
                ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

                List<String> holdingsRecordsIds = holdingsRecords.stream().map(HoldingsRecord::getId).toList();
                List<JsonObject> validatedItems = validateItems(jsons, holdingsRecordsIds, notUpdatedEntities);

                List<Item> items = validatedItems.stream()
                  .map(itemJson -> mapToItem(itemJson, toHoldingsRecord.getId(), true)).toList();

                return validateItemsBoundWith(items, notUpdatedEntities, routingContext, context)
                  .thenCompose(wrappersWithoutBoundWith -> createItems(wrappersWithoutBoundWith, notUpdatedEntities, Item::getId, targetTenantItemCollection))
                  .thenCompose(createdItems -> deleteSourceItems(createdItems, notUpdatedEntities, Item::getId, sourceTenantItemCollection));
              });
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("updateOwnershipOfItems:: Error during update ownership of items {}, to tenant: {}",
        itemsUpdateOwnership.getItemIds(), itemsUpdateOwnership.getTargetTenantId(), e);

      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<String>> updateOwnershipOfHoldingsRecords(HoldingsUpdateOwnership holdingsUpdateOwnership,
                                                                           List<NotUpdatedEntity> notUpdatedEntities, RoutingContext routingContext,
                                                                           WebContext context, Context targetTenantContext) {
    LOGGER.info("updateOwnershipOfHoldingsRecords:: Starting ownership update for holdings: {}, source tenant: {}, target tenant: {}",
      holdingsUpdateOwnership.getHoldingsRecordIds(), context.getTenantId(), targetTenantContext.getTenantId());

    try {
      // Initialize clients and collections
      HoldingsOwnershipUpdateContext updateContext = initializeUpdateContext(routingContext, context, targetTenantContext);

      return fetchAndValidateHoldingsRecords(holdingsUpdateOwnership, notUpdatedEntities, updateContext)
        .thenCompose(validatedHoldings -> {
          if (validatedHoldings.isEmpty()) {
            LOGGER.info("updateOwnershipOfHoldingsRecords:: No valid holdings records found to update");
            return CompletableFuture.completedFuture(new ArrayList<>());
          }

          return processHoldingsOwnershipUpdate(validatedHoldings, notUpdatedEntities, updateContext);
        })
        .exceptionally(throwable -> {
          LOGGER.error("updateOwnershipOfHoldingsRecords:: Failed to update holdings ownership for holdings: {}, target tenant: {}",
            holdingsUpdateOwnership.getHoldingsRecordIds(), targetTenantContext.getTenantId(), throwable);
          throw new CompletionException(throwable);
        });

    } catch (Exception e) {
      LOGGER.error("updateOwnershipOfHoldingsRecords:: Unexpected error during holdings ownership update for holdings: {}, target tenant: {}",
        holdingsUpdateOwnership.getHoldingsRecordIds(), targetTenantContext.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Fetches and validates holdings records from the source tenant
   */
  CompletableFuture<List<HoldingsRecord>> fetchAndValidateHoldingsRecords(HoldingsUpdateOwnership holdingsUpdateOwnership,
                                                                          List<NotUpdatedEntity> notUpdatedEntities, HoldingsOwnershipUpdateContext updateContext) {

    LOGGER.debug("fetchAndValidateHoldingsRecords:: Fetching holdings records: {}", holdingsUpdateOwnership.getHoldingsRecordIds());

    return updateContext.holdingsRecordFetchClient
      .find(holdingsUpdateOwnership.getHoldingsRecordIds(), MoveApiUtil::fetchByIdCql)
      .thenCompose(jsons -> {
        LOGGER.info("fetchAndValidateHoldingsRecords:: Found {} holdings records to process", jsons.size());

        // Process not found entities
        processNotFoundEntities(holdingsUpdateOwnership.getHoldingsRecordIds(),
          notUpdatedEntities, updateContext.sourceContext, jsons, HOLDINGS_RECORD_NOT_FOUND);

        if (jsons.isEmpty()) {
          return CompletableFuture.completedFuture(new ArrayList<HoldingsRecord>());
        }

        // Validate and transform holdings records
        List<JsonObject> validatedHoldingsRecords = validateHoldingsRecords(
          jsons, holdingsUpdateOwnership.getToInstanceId(), notUpdatedEntities);

        List<HoldingsRecord> holdingsRecords = validatedHoldingsRecords.stream()
          .map(h -> mapToHoldingsRecord(h, holdingsUpdateOwnership))
          .toList();

        LOGGER.debug("fetchAndValidateHoldingsRecords:: Validated {} holdings records", holdingsRecords.size());

        return CompletableFuture.completedFuture(holdingsRecords);
      })
      .exceptionally(throwable -> {
        LOGGER.error("fetchAndValidateHoldingsRecords:: Failed to fetch holdings records: {}",
          holdingsUpdateOwnership.getHoldingsRecordIds(), throwable);
        throw new CompletionException(throwable);
      });
  }

  /**
   * Main processing method for holdings ownership update
   */
  CompletableFuture<List<String>> processHoldingsOwnershipUpdate(List<HoldingsRecord> holdingsRecords, List<NotUpdatedEntity> notUpdatedEntities,
                                                                 HoldingsOwnershipUpdateContext updateContext) {

    LOGGER.debug("processHoldingsOwnershipUpdate:: Processing {} holdings records", holdingsRecords.size());

    return fetchMarcSourceRecords(holdingsRecords, notUpdatedEntities, updateContext)
      .thenCompose(holdingMarcSources -> {
        holdingsRecords.forEach(h -> h.setHrid(null));

        Set<String> failedHoldingsIds = notUpdatedEntities.stream()
          .map(NotUpdatedEntity::getEntityId)
          .collect(Collectors.toSet());

        List<HoldingsRecord> holdingsToProcessFurther = holdingsRecords.stream()
          .filter(h -> !failedHoldingsIds.contains(h.getId()))
          .toList();

        if (holdingsToProcessFurther.isEmpty()) {
          LOGGER.info("processHoldingsOwnershipUpdate:: No holdings to process after initial validation.");
          return CompletableFuture.completedFuture(new ArrayList<>());
        }

        return validateHoldingsRecordsBoundWith(holdingsToProcessFurther, notUpdatedEntities, updateContext.routingContext, updateContext.sourceContext)
          .thenCompose(validatedHoldings -> {
            if (validatedHoldings.isEmpty()) {
              LOGGER.info("processHoldingsOwnershipUpdate:: No holdings to process");
              return CompletableFuture.completedFuture(Collections.emptyList());
            }
            return createHoldingsInTargetTenant(validatedHoldings, notUpdatedEntities, updateContext)
              .thenCompose(createdHoldings -> {
                LOGGER.info("processHoldingsOwnershipUpdate:: Created {} holdings in target tenant: {}",
                  createdHoldings.size(), updateContext.targetTenantContext.getTenantId());

                List<HoldingsRecord> holdingsToDelete = getHoldingsToDelete(notUpdatedEntities, validatedHoldings);

                return moveSrsRecordsForMarcHoldings(holdingsRecords, createdHoldings, updateContext.sourceContext, updateContext.targetTenantContext, notUpdatedEntities, holdingMarcSources)
                  .thenCompose(v -> transferAttachedItems(createdHoldings, notUpdatedEntities, updateContext.routingContext, updateContext.sourceContext, updateContext.targetTenantContext))
                  .thenCompose(itemIds -> deleteSourceHoldings(holdingsToDelete, notUpdatedEntities, updateContext.sourceTenantHoldingsRecordCollection));
              });
          });
      })
      .exceptionally(throwable -> {
        LOGGER.error("processHoldingsOwnershipUpdate:: Failed to process holdings ownership update", throwable);
        throw new CompletionException(throwable);
      });
  }

  /**
   * Fetches MARC source records for holdings that have MARC type.
   * This method is designed to be resilient: if a source record for a specific holdings
   * cannot be fetched, it will be added to the notUpdatedEntities list, and the overall
   * operation will continue for all other valid holdings without throwing an exception.
   */
  CompletableFuture<Map<String, Record>> fetchMarcSourceRecords(List<HoldingsRecord> holdingsRecords,
                                                                List<NotUpdatedEntity> notUpdatedEntities,
                                                                HoldingsOwnershipUpdateContext updateContext) {

    LOGGER.debug("fetchMarcSourceRecords:: Fetching MARC source records for {} holdings", holdingsRecords.size());

    List<HoldingsRecord> marcHoldings = holdingsRecords.stream()
      .filter(h -> MARC_SOURCE_ID.equals(h.getSourceId()))
      .toList();

    if (marcHoldings.isEmpty()) {
      LOGGER.debug("fetchMarcSourceRecords:: No MARC holdings to process.");
      return CompletableFuture.completedFuture(new HashMap<>());
    }

    Map<String, Record> holdingMarcSources = new HashMap<>();
    List<CompletableFuture<Void>> srsFutures = marcHoldings.stream()
      .map(h -> getSourceRecordByExternalId(h.getId(), updateContext.sourceSrsClient)
        .thenAccept(record -> {
          if (record != null) {
            holdingMarcSources.put(h.getId(), record);
          } else {
            String errorMessage = "Failed to fetch MARC source record for holdings id: " + h.getId();
            LOGGER.warn(errorMessage);
            notUpdatedEntities.add(new NotUpdatedEntity()
              .withEntityId(h.getId())
              .withErrorMessage(errorMessage));
          }
        }))
      .toList();

    return CompletableFuture.allOf(srsFutures.toArray(new CompletableFuture[0]))
      .thenApply(v -> holdingMarcSources);
  }

  /**
   * Moves SRS records for MARC holdings to the target tenant.
   * This method only processes holdings that are of MARC source type and for which
   * a source record was successfully fetched in the previous step.
   *
   * @param sourceHoldings     The original list of holdings records from the source tenant.
   * @param targetHoldings     The list of newly created holdings records in the target tenant.
   * @param sourceContext      The context for the source tenant.
   * @param targetTenantContext The context for the target tenant.
   * @param notUpdatedEntities A list to which any entities that fail during processing will be added.
   * @param holdingMarcSources A map of source holdings ID to its fetched MARC Record.
   * @return A CompletableFuture that completes when all SRS records have been processed.
   */
  CompletableFuture<Void> moveSrsRecordsForMarcHoldings(List<HoldingsRecord> sourceHoldings, List<HoldingsRecord> targetHoldings,
                                                        WebContext sourceContext, Context targetTenantContext, List<NotUpdatedEntity> notUpdatedEntities,
                                                        Map<String, Record> holdingMarcSources) {

    LOGGER.debug("moveSrsRecordsForMarcHoldings:: Starting SRS record migration for {} source holdings", sourceHoldings.size());

    // Filter to get only MARC holdings for which we have successfully fetched an SRS record.
    Set<String> successfullyFetchedHoldingsIds = holdingMarcSources.keySet();
    List<HoldingsRecord> marcHoldingsToProcess = sourceHoldings.stream()
      .filter(h -> MARC_SOURCE_ID.equals(h.getSourceId()))
      .filter(h -> successfullyFetchedHoldingsIds.contains(h.getId()))
      .toList();

    if (marcHoldingsToProcess.isEmpty()) {
      LOGGER.info("moveSrsRecordsForMarcHoldings:: No MARC holdings with fetched SRS records found to process.");
      return CompletableFuture.completedFuture(null);
    }

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(java.util.UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    return snapshotService.postSnapshotInSrsAndHandleResponse(targetTenantContext, snapshot)
      .toCompletionStage().toCompletableFuture()
      .thenCompose(createdSnapshot -> {
        LOGGER.info("moveSrsRecordsForMarcHoldings:: Created a single snapshot {} for the entire operation", createdSnapshot.getJobExecutionId());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (HoldingsRecord sourceHolding : marcHoldingsToProcess) {
          LOGGER.debug("moveSrsRecordsForMarcHoldings:: Processing MARC holdings: {}", sourceHolding.getId());
          HoldingsRecord targetHolding = findMatchingTargetHolding(sourceHolding, targetHoldings);
          if (targetHolding == null) {
            String msg = String.format("No matching target holding found for source holding id: %s", sourceHolding.getId());
            LOGGER.warn("moveSrsRecordsForMarcHoldings:: {}", msg);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHolding.getId()).withErrorMessage(msg));
            continue;
          }

          Record sourceRecord = holdingMarcSources.get(sourceHolding.getId());
          if (sourceRecord == null) {
            String msg = String.format("No MARC source record found for holdings id: %s", sourceHolding.getId());
            LOGGER.warn("moveSrsRecordsForMarcHoldings:: {}", msg);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHolding.getId()).withErrorMessage(msg));
            continue;
          }

          LOGGER.debug("moveSrsRecordsForMarcHoldings:: Moving SRS record for source holdings: {} to target holdings: {}",
            sourceHolding.getId(), targetHolding.getId());

          futures.add(moveSingleMarcHoldingsSrsRecord(sourceHolding, sourceRecord, targetHolding,
            sourceContext, targetTenantContext, notUpdatedEntities, createdSnapshot));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      })
      .thenAccept(v -> LOGGER.info("moveSrsRecordsForMarcHoldings:: Successfully processed MARC SRS records"))
      .exceptionally(throwable -> {
        LOGGER.error("moveSrsRecordsForMarcHoldings:: Failed to move SRS records", throwable);
        if (throwable.getCause() instanceof BadRequestException || throwable.getCause() instanceof NotFoundException) {
          marcHoldingsToProcess.forEach(h -> notUpdatedEntities.add(new NotUpdatedEntity()
            .withEntityId(h.getId())
            .withErrorMessage("Failed to create a snapshot for the operation: " + throwable.getMessage())));
        }
        throw new CompletionException(throwable);
      });
  }

  /**
   * Finds a matching target holding for the given source holding
   */
  HoldingsRecord findMatchingTargetHolding(HoldingsRecord sourceHolding, List<HoldingsRecord> targetHoldings) {
    return targetHoldings.stream()
      .filter(h -> h.getPermanentLocationId().equals(sourceHolding.getPermanentLocationId())
        && h.getInstanceId().equals(sourceHolding.getInstanceId()))
      .findFirst()
      .orElse(null);
  }

  CompletableFuture<Void> moveSingleMarcHoldingsSrsRecord(HoldingsRecord sourceHolding, Record record, HoldingsRecord targetHolding,
                                                          WebContext sourceContext, Context targetTenantContext, List<NotUpdatedEntity> notUpdatedEntities, Snapshot snapshot) {

    LOGGER.debug("moveSingleMarcHoldingsSrsRecord:: Starting SRS record migration for holdings: {} -> {}",
      sourceHolding.getId(), targetHolding.getId());

    CompletableFuture<Void> result = new CompletableFuture<>();

    try {
      SourceStorageRecordsClientWrapper sourceSrsClient = clientFactory.createSourceStorageRecordsClient(sourceContext, client);
      SourceStorageRecordsClientWrapper targetSrsClient = clientFactory.createSourceStorageRecordsClient(targetTenantContext, client);

      Record newRecordForTarget = new Record()
        .withSnapshotId(snapshot.getJobExecutionId())
        .withMatchedId(record.getMatchedId())
        .withRecordType(record.getRecordType())
        .withParsedRecord(record.getParsedRecord())
        .withExternalIdsHolder(record.getExternalIdsHolder())
        .withAdditionalInfo(record.getAdditionalInfo())
        .withRawRecord(record.getRawRecord());

      targetSrsClient.postSourceStorageRecords(newRecordForTarget).onComplete(postAr -> {
        if (postAr.failed() || postAr.result().statusCode() != HttpStatus.HTTP_CREATED.toInt()) {
          String msg = String.format("Failed to post SRS record to target tenant=%s: %s",
            targetTenantContext.getTenantId(), postAr.cause() != null ? postAr.cause().getMessage() : postAr.result().bodyAsString());
          LOGGER.warn("moveSingleMarcHoldingsSrsRecord:: {}", msg);
          notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHolding.getId()).withErrorMessage(msg));
          result.complete(null);
          return;
        }

        LOGGER.debug("moveSingleMarcHoldingsSrsRecord:: Posted SRS record to target tenant={}, response: {}",
          targetTenantContext.getTenantId(), postAr.result().bodyAsString());

        // Mark SRS record as deleted in source tenant
        record.setState(Record.State.DELETED);
        record.setDeleted(true);

        LOGGER.debug("moveSingleMarcHoldingsSrsRecord:: Marking SRS record as deleted in source tenant for holdings: {}", sourceHolding.getId());

        sourceSrsClient.putSourceStorageRecordsById(record.getId(), record).onComplete(putAr -> {
          if (putAr.failed() || (putAr.result().statusCode() != HttpStatus.HTTP_OK.toInt() && putAr.result().statusCode() != HttpStatus.HTTP_NO_CONTENT.toInt())) {
            String msg = String.format("Failed to mark holdings source as deleted in source tenant=%s: %s",
              sourceContext.getTenantId(), putAr.cause() != null ? putAr.cause().getMessage() : putAr.result().bodyAsString());
            LOGGER.warn("moveSingleMarcHoldingsSrsRecord:: {}", msg);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHolding.getId()).withErrorMessage(msg));
            result.complete(null);
            return;
          }

          LOGGER.debug("moveSingleMarcHoldingsSrsRecord:: Marked SRS record as deleted in source tenant={}, response: {}",  sourceContext.getTenantId(), putAr.result().bodyAsString());
          LOGGER.info("moveSingleMarcHoldingsSrsRecord:: Successfully migrated SRS record for holdings: {} -> {}", sourceHolding.getId(), targetHolding.getId());
          result.complete(null);
        });
      });
    } catch (Exception e) {
      String msg = String.format("Exception during moving holdingsId=%s: %s", sourceHolding.getId(), e.getMessage());
      LOGGER.error("moveSingleMarcHoldingsSrsRecord:: {}", msg, e);
      notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(sourceHolding.getId()).withErrorMessage(msg));
      result.complete(null);
    }

    return result;
  }

  /**
   * Fetches source record by external ID from the source storage records client
   */
  CompletableFuture<Record> getSourceRecordByExternalId(String externalId, SourceStorageRecordsClientWrapper srsClient) {
    LOGGER.debug("getSourceRecordByExternalId:: Fetching source record by externalId: {}", externalId);

    CompletableFuture<Record> future = new CompletableFuture<>();
    try {
      srsClient.getSourceStorageRecordsFormattedById(externalId, "HOLDINGS")
        .onSuccess(response -> {
          try {
            if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
              JsonObject responseBody = response.bodyAsJsonObject();
              LOGGER.debug("getSourceRecordByExternalId:: Response received for externalId '{}': status={}", externalId, response.statusCode());

              // Remove "formattedContent" from parsedRecord if present
              JsonObject parsedRecord = responseBody.getJsonObject("parsedRecord");
              if (parsedRecord != null && parsedRecord.containsKey("formattedContent")) {
                parsedRecord.remove("formattedContent");
                LOGGER.debug("getSourceRecordByExternalId:: Removed formattedContent from parsedRecord for externalId: {}", externalId);
              }

              Record foundRecord = responseBody.mapTo(Record.class);
              LOGGER.debug("getSourceRecordByExternalId:: Successfully mapped record for externalId '{}'", externalId);
              future.complete(foundRecord);
            } else {
              String errorMessage = format("Failed to fetch source record by externalId '%s'. Status: %d, Body: %s",
                externalId, response.statusCode(), response.bodyAsString());
              LOGGER.warn("getSourceRecordByExternalId:: {}", errorMessage);
              future.complete(null);
            }
          } catch (Exception e) {
            LOGGER.error("getSourceRecordByExternalId:: Error processing response for externalId '{}'", externalId, e);
            future.complete(null);
          }
        })
        .onFailure(error -> {
          LOGGER.error("getSourceRecordByExternalId:: Error querying record-storage for source record with externalId '{}'", externalId, error);
          future.complete(null);
        });
    } catch (Exception e) {
      LOGGER.error("getSourceRecordByExternalId:: Unexpected error for externalId '{}'", externalId, e);
      future.complete(null);
    }

    return future;
  }

  private CompletableFuture<List<String>> transferAttachedItems(List<HoldingsRecord> holdingsRecordsWrappers,
                                                                List<NotUpdatedEntity> notUpdatedEntities,
                                                                RoutingContext routingContext, WebContext context, Context targetTenantContext) {
    List<String> sourceHoldingsRecordsIds = new ArrayList<>();

    try {
      sourceHoldingsRecordsIds = holdingsRecordsWrappers.stream().map(HoldingsRecord::getId).toList();

      LOGGER.debug("transferAttachedItems:: Transfer items of holdingsRecordIds: {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId());

      CollectionResourceClient itemsStorageClient = createItemStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient itemsFetchClient = createItemsFetchClient(itemsStorageClient);

      ItemCollection sourceTenantItemCollection = storage.getItemCollection(context);
      ItemCollection targetTenantItemCollection = storage.getItemCollection(targetTenantContext);

      return itemsFetchClient.find(sourceHoldingsRecordsIds, MoveApiUtil::fetchByHoldingsRecordIdCql)
        .thenApply(jsons -> jsons.stream().map(itemJson -> {
          String targetHoldingId = getTargetHoldingId(itemJson, holdingsRecordsWrappers);
          return mapToItem(itemJson, targetHoldingId, false);
        }).toList())
        .thenCompose(items -> {
          if (!items.isEmpty()) {
            LOGGER.debug("transferAttachedItems:: Found items to transfer: {}", items);
            return createItems(items, notUpdatedEntities, Item::getHoldingId, targetTenantItemCollection)
              .thenCompose(createdItems -> deleteSourceItems(createdItems, notUpdatedEntities, Item::getHoldingId, sourceTenantItemCollection));
          }
          return CompletableFuture.completedFuture(new ArrayList<>());
        });
    } catch (Exception e) {
      LOGGER.warn("transferAttachedItems:: Error during transfer attached items for holdings {}, to tenant: {}",
        sourceHoldingsRecordsIds, targetTenantContext.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Creates holdings records in the target tenant
   */
  CompletableFuture<List<HoldingsRecord>> createHoldingsInTargetTenant(List<HoldingsRecord> holdingsRecords, List<NotUpdatedEntity> notUpdatedEntities,
                                                                       HoldingsOwnershipUpdateContext updateContext) {

    LOGGER.debug("createHoldingsInTargetTenant:: Creating {} holdings in target tenant", holdingsRecords.size());

    return createHoldings(holdingsRecords, notUpdatedEntities, updateContext.targetTenantHoldingsRecordCollection)
      .exceptionally(throwable -> {
        LOGGER.error("createHoldingsInTargetTenant:: Failed to create holdings in target tenant", throwable);
        throw new CompletionException(throwable);
      });
  }

  private CompletableFuture<List<Item>> createItems(List<Item> items,
                                                    List<NotUpdatedEntity> notUpdatedEntities,
                                                    Function<Item, String> getEntityIdForError,
                                                    ItemCollection itemCollection) {
    LOGGER.debug("createItems:: Creating items: {}", items);

    List<CompletableFuture<Item>> createFutures = items.stream()
      .map(item ->
        itemCollection.add(item)
          .exceptionally(e -> {
            LOGGER.warn("createItems:: Error during creating item with id: {} for holdingsRecord with id: {}", item.getId(), item.getHoldingId(), e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(getEntityIdForError.apply(item)).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(i -> item))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<HoldingsRecord>> createHoldings(List<HoldingsRecord> holdingsRecords,
                                                                 List<NotUpdatedEntity> notUpdatedEntities,
                                                                 HoldingsRecordCollection holdingsRecordCollection) {

    LOGGER.debug("createHoldings:: Creating holdings records: {}", holdingsRecords);

    List<CompletableFuture<HoldingsRecord>> createFutures = holdingsRecords.stream()
      .map(holdingsRecord ->
        holdingsRecordCollection.add(holdingsRecord)
          .exceptionally(e -> {
            LOGGER.warn("createHoldings:: Error during creating holdingsRecord with id: {}", holdingsRecord, e);
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecord.getId()).withErrorMessage(e.getMessage()));
            throw new CompletionException(e);
          }).thenApply(h -> holdingsRecord))
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteSourceHoldings(List<HoldingsRecord> holdingsRecords,
                                                               List<NotUpdatedEntity> notUpdatedEntities,
                                                               HoldingsRecordCollection holdingsRecordCollection) {

    LOGGER.debug("deleteSourceHoldings:: Deleting holdings record with ids {}",
      holdingsRecords.stream().map(HoldingsRecord::getId).toList());

    List<CompletableFuture<String>> deleteFutures = holdingsRecords.stream()
      .map(holdingsRecord -> {
        CompletableFuture<String> future = new CompletableFuture<>();
        holdingsRecordCollection.delete(holdingsRecord.getId(),
          success -> {
            LOGGER.debug("deleteSourceHoldings:: Successfully deleted holdingsRecord with id: {}", holdingsRecord.getId());
            future.complete(holdingsRecord.getId());
          },
          failure -> {
            LOGGER.warn("deleteSourceHoldings:: Error during deleting holdingsRecord with id: {}, status code: {}, reason: {}",
              holdingsRecord.getId(), failure.getStatusCode(), failure.getReason());

            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(holdingsRecord.getId()).withErrorMessage(failure.getReason()));
            future.complete(null);
          });
        return future;
      }).toList();

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
      .thenApply(v -> deleteFutures.stream()
        .map(CompletableFuture::join)
        .filter(Objects::nonNull)
        .collect(Collectors.toList()));
  }

  private CompletableFuture<List<String>> deleteSourceItems(List<Item> items,
                                                            List<NotUpdatedEntity> notUpdatedEntities,
                                                            Function<Item, String> getEntityIdForError,
                                                            ItemCollection itemCollection) {

    LOGGER.debug("deleteSourceItems:: Deleting items with ids {}", items.stream().map(Item::getId).toList());

    List<CompletableFuture<String>> deleteFutures = items.stream()
      .map(item -> {
        CompletableFuture<String> future = new CompletableFuture<>();
        itemCollection.delete(item.getId(), success -> {
            LOGGER.debug("deleteSourceItems:: Successfully deleted item with id: {}", item.getId());
            future.complete(item.getId());
          },
          failure -> {
            LOGGER.warn("deleteSourceItems:: Error during deleting item with id: {} for holdingsRecord with id {}, status code: {}, reason: {}",
              item.getId(), item.getHoldingId(), failure.getStatusCode(), failure.getReason());
            notUpdatedEntities.add(new NotUpdatedEntity().withEntityId(getEntityIdForError.apply(item)).withErrorMessage(failure.getReason()));
            future.complete(null);
          });
        return future;
      }).toList();

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
      .thenApply(v -> deleteFutures.stream()
        .map(CompletableFuture::join)
        .filter(Objects::nonNull)
        .collect(Collectors.toList()));
  }

  private CompletableFuture<List<HoldingsRecord>> validateHoldingsRecordsBoundWith(List<HoldingsRecord> holdingsRecords,
                                                                                   List<NotUpdatedEntity> notUpdatedEntities,
                                                                                   RoutingContext routingContext, WebContext context) {

    LOGGER.debug("validateHoldingsRecordsBoundWith:: Validating holdings records bound with parts: {}", holdingsRecords);

    try {
      List<String> sourceHoldingsRecordsIds = holdingsRecords.stream().map(HoldingsRecord::getId).toList();

      CollectionResourceClient boundWithPartsStorageClient = createBoundWithPartsStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient boundWithPartsFetchClient = createBoundWithPartsFetchClient(boundWithPartsStorageClient);

      return boundWithPartsFetchClient.find(sourceHoldingsRecordsIds, MoveApiUtil::fetchByHoldingsRecordIdCql)
        .thenApply(jsons -> {
          List<String> boundWithHoldingsRecordsIds =
            jsons.stream()
              .map(boundWithPart -> boundWithPart.getString(HOLDINGS_RECORD_ID))
              .distinct()
              .toList();

          return holdingsRecords.stream().filter(holdingsRecord -> {
            if (boundWithHoldingsRecordsIds.contains(holdingsRecord.getId())) {
              notUpdatedEntities.add(new NotUpdatedEntity()
                .withErrorMessage(String.format(HOLDING_BOUND_WITH_PARTS_ERROR, holdingsRecord.getId()))
                .withEntityId(holdingsRecord.getId()));
              return false;
            }
            return true;
          }).toList();
        });
    } catch (Exception e) {
      LOGGER.warn("validateHoldingsRecordsBoundWith:: Error during  validating holdings record bound with part, tenant: {}", context.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<Item>> validateItemsBoundWith(List<Item> items,
                                                               List<NotUpdatedEntity> notUpdatedEntities,
                                                               RoutingContext routingContext, WebContext context) {
    try {
      List<String> sourceItemsIds = items.stream().map(Item::getId).toList();

      CollectionResourceClient boundWithPartsStorageClient = createBoundWithPartsStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient boundWithPartsFetchClient = createBoundWithPartsFetchClient(boundWithPartsStorageClient);

      return boundWithPartsFetchClient.find(sourceItemsIds, MoveApiUtil::fetchByItemIdCql)
        .thenApply(jsons -> {
          List<String> boundWithItemsIds =
            jsons.stream()
              .map(boundWithPart -> boundWithPart.getString(ITEM_ID))
              .distinct()
              .toList();

          return items.stream().filter(item -> {
            if (boundWithItemsIds.contains(item.getId())) {
              notUpdatedEntities.add(new NotUpdatedEntity()
                .withErrorMessage(String.format(ITEM_WITH_PARTS_ERROR, item.getId()))
                .withEntityId(item.getId()));
              return false;
            }
            return true;
          }).toList();
        });
    } catch (Exception e) {
      LOGGER.warn("validateItemsBoundWith:: Error during  validating items bound with part, tenant: {}", context.getTenantId(), e);
      return CompletableFuture.failedFuture(e);
    }
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

  private String getTargetHoldingId(JsonObject itemJson, List<HoldingsRecord> holdingsRecordsWrappers) {
    return holdingsRecordsWrappers.stream()
      .filter(h -> h.getId().equals(itemJson.getString(HOLDINGS_RECORD_ID))).findFirst().map(HoldingsRecord::getId)
      .orElse(null);
  }

  private HoldingsRecord mapToHoldingsRecord(JsonObject holdingsRecordJson, HoldingsUpdateOwnership holdingsUpdateOwnership) {
    MoveApiUtil.removeExtraRedundantFields(holdingsRecordJson);

    return holdingsRecordJson.mapTo(HoldingsRecord.class)
      .withHrid(null)
      .withPermanentLocationId(holdingsUpdateOwnership.getTargetLocationId());
  }

  /**
   * Maps item JSON representation to an Item object.
   * Order cleanup should be performed only for item update ownership request.
   *
   * @param itemJson the item JSON representation
   * @param targetHoldingId the ID of the target holdings record
   * @param cleanUpOrder if true, the order information will be removed from the item representation
   * @return an Item object with the specified target holdings ID and cleaned up order if required
   */
  private Item mapToItem(JsonObject itemJson, String targetHoldingId, boolean cleanUpOrder) {
    var item = ItemUtil.fromStoredItemRepresentation(itemJson)
      .withHrid(null)
      .withHoldingId(targetHoldingId)
      .withTemporaryLocationId(null)
      .withPermanentLocationId(null);
    return cleanUpOrder ? item.withOrder(null) : item;
  }

  private List<HoldingsRecord> getHoldingsToDelete(List<NotUpdatedEntity> notUpdatedEntities,
                                                   List<HoldingsRecord> holdingsRecords) {
    List<String> notUpdatedHoldingsIds = notUpdatedEntities.stream().map(NotUpdatedEntity::getEntityId).toList();
    return holdingsRecords.stream().filter(holdingsRecord -> !notUpdatedHoldingsIds.contains(holdingsRecord.getId())).toList();
  }

  /**
   * Initializes all clients and collections needed for the update process
   */
  HoldingsOwnershipUpdateContext initializeUpdateContext(RoutingContext routingContext,
                                                         WebContext context,
                                                         Context targetTenantContext) {

    LOGGER.debug("initializeUpdateContext:: Initializing update context");

    MultipleRecordsFetchClient holdingsRecordFetchClient = clientFactory.createHoldingsRecordsFetchClient(routingContext, context, client);
    SourceStorageRecordsClientWrapper sourceSrsClient = clientFactory.createSourceStorageRecordsClient(context, client);

    HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);
    HoldingsRecordCollection targetTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(targetTenantContext);

    LOGGER.debug("initializeUpdateContext:: Update context initialized successfully");

    return new HoldingsOwnershipUpdateContext(holdingsRecordFetchClient, sourceSrsClient,
      sourceTenantHoldingsRecordCollection, targetTenantHoldingsRecordCollection,
      context, targetTenantContext, routingContext);

  }

  /**
   * Context class to hold all the clients and collections needed for the update process
   */
  record HoldingsOwnershipUpdateContext(
    MultipleRecordsFetchClient holdingsRecordFetchClient,
    SourceStorageRecordsClientWrapper sourceSrsClient,
    HoldingsRecordCollection sourceTenantHoldingsRecordCollection,
    HoldingsRecordCollection targetTenantHoldingsRecordCollection,
    WebContext sourceContext, Context targetTenantContext,
    RoutingContext routingContext) {
  }
}
