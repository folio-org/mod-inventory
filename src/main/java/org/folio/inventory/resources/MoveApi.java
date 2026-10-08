package org.folio.inventory.resources;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsRecordsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createHttpClient;
import static org.folio.inventory.support.MoveApiUtil.createItemStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createItemsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.respond;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.MoveValidator.holdingsMoveHasRequiredFields;
import static org.folio.inventory.validation.MoveValidator.itemsMoveHasRequiredFields;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.MoveApiUtil;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.rest.jaxrs.model.HoldingsRecord;

public class MoveApi extends AbstractInventoryResource {
  public static final String TO_HOLDINGS_RECORD_ID = "toHoldingsRecordId";
  public static final String TO_INSTANCE_ID = "toInstanceId";
  public static final String ITEM_IDS = "itemIds";
  public static final String HOLDINGS_RECORD_IDS = "holdingsRecordIds";
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  private static final String INSTANCE_NOT_FOUND = "Instance with id=%s not found";
  private final ConsortiumService consortiumService;

  public MoveApi(final Storage storage, final HttpClient client, ConsortiumService consortiumService) {
    super(storage, client);
    this.consortiumService = consortiumService;
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/holdings*")
      .handler(BodyHandler.create());
    router.post("/inventory/items/move")
      .handler(this::moveItems);
    router.post("/inventory/holdings/move")
      .handler(this::moveHoldings);
  }

  private void moveItems(RoutingContext routingContext) {
    LOGGER.info("moveItems:: Starting items move operation.");
    final var context = new WebContext(routingContext);
    final var itemsMoveJsonRequest = routingContext.body().asJsonObject();

    final var validationError = itemsMoveHasRequiredFields(itemsMoveJsonRequest);

    if (validationError.isPresent()) {
      LOGGER.warn("moveItems:: Validation error: {}", validationError.get().message());
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    final var toHoldingsRecordId = itemsMoveJsonRequest.getString(TO_HOLDINGS_RECORD_ID);
    final var itemIdsToUpdate = toListOfStrings(itemsMoveJsonRequest, ITEM_IDS);

    storage.getHoldingsRecordCollection(context)
      .findById(toHoldingsRecordId)
      .thenAccept(holding -> {
        if (holding != null) {
          try {
            final var itemsStorageClient =
              createItemStorageClient(createHttpClient(client, routingContext, context), context);
            final var itemsFetchClient = createItemsFetchClient(itemsStorageClient);

            itemsFetchClient.find(itemIdsToUpdate, MoveApiUtil::fetchByIdCql)
              .thenCombine(fetchMaxOrder(itemsStorageClient, toHoldingsRecordId),
                (jsons, maxOrder) -> updateItemFields(toHoldingsRecordId, jsons, maxOrder))
              .thenAccept(itemsToUpdate -> updateItems(routingContext, context, itemIdsToUpdate, itemsToUpdate))
              .exceptionally(e -> {
                LOGGER.error("moveItems:: Failed to fetch items for moving. IDs: {}", itemIdsToUpdate, e);
                ServerErrorResponse.internalError(routingContext.response(), e);
                return null;
              });
          } catch (Exception e) {
            LOGGER.error("moveItems:: Failed moving items to holding with id={}.", toHoldingsRecordId, e);
            ServerErrorResponse.internalError(routingContext.response(), e);
          }
        } else {
          LOGGER.error("moveItems:: Holding with id={} not found. Aborting move operation.", toHoldingsRecordId);
          unprocessableEntity(routingContext.response(), format("Holding with id=%s not found", toHoldingsRecordId));
        }
      })
      .exceptionally(e -> {
        LOGGER.error("moveItems:: Failed to complete move items operation for holdingsRecordId {}", toHoldingsRecordId,
          e);
        ServerErrorResponse.internalError(routingContext.response(), e);
        return null;
      });
  }

  @SuppressWarnings("checkstyle:MethodLength")
  private void moveHoldings(RoutingContext routingContext) {
    LOGGER.info("moveHoldings:: Staring holdings move operation.");
    WebContext context = new WebContext(routingContext);
    JsonObject holdingsMoveJsonRequest = routingContext.body().asJsonObject();

    Optional<ValidationError> validationError = holdingsMoveHasRequiredFields(holdingsMoveJsonRequest);
    if (validationError.isPresent()) {
      LOGGER.warn("moveHoldings:: Validation error: {}", validationError.get().message());
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    String toInstanceId = holdingsMoveJsonRequest.getString(TO_INSTANCE_ID);
    List<String> holdingsRecordsIdsToUpdate =
      toListOfStrings(holdingsMoveJsonRequest.getJsonArray(HOLDINGS_RECORD_IDS));

    LOGGER.info("moveHoldings:: Attempting to move {} holdings records to instanceId {}",
      holdingsRecordsIdsToUpdate.size(), toInstanceId);

    storage.getInstanceCollection(context)
      .findById(toInstanceId)
      .handle((localInstance, error) -> {
        if (error != null) {
          LOGGER.error("moveHoldings:: Failed to query local instance storage for instanceId {}", toInstanceId, error);
          throw new CompletionException(error);
        }
        if (localInstance != null) {
          LOGGER.info("moveHoldings:: Instance {} found locally.", toInstanceId);
        }
        return localInstance;
      })
      .thenCompose(localInstance -> {
        if (localInstance != null) {
          return CompletableFuture.completedFuture(localInstance);
        }
        return findInstanceInConsortium(context, toInstanceId);
      })
      .thenAccept(foundInstance -> {
        if (foundInstance == null) {
          LOGGER.warn("moveHoldings:: Instance {} not found locally or in consortium. Aborting move operation.",
            toInstanceId);
          throw new BadRequestException(format(INSTANCE_NOT_FOUND, toInstanceId));
        }
        LOGGER.info("moveHoldings:: Target instance {} found. Proceeding to update holdings records.",
          foundInstance.getId());
        updateHoldingsForInstance(routingContext, context, foundInstance, holdingsRecordsIdsToUpdate);
      })
      .exceptionally(e -> {
        if (e.getCause() instanceof BadRequestException) {
          LOGGER.error("moveHoldings:: Bad request while attempting to move holdings to instanceId {}: {}",
            toInstanceId, e.getCause().getMessage());
          unprocessableEntity(routingContext.response(), e.getCause().getMessage());
        } else {
          LOGGER.error("moveHoldings:: Failed to complete move holdings operation for instanceId {}", toInstanceId, e);
          ServerErrorResponse.internalError(routingContext.response(), e);
        }
        return null;
      });
  }

  private void updateHoldingsForInstance(RoutingContext routingContext, WebContext context, Instance instance,
                                         List<String> holdingsRecordsIdsToUpdate) {
    LOGGER.info("updateHoldingsForInstance:: Preparing to update {} holdings records to point to instanceId {}.",
      holdingsRecordsIdsToUpdate.size(), instance.getId());
    try {
      CollectionResourceClient holdingsStorageClient =
        createHoldingsStorageClient(createHttpClient(client, routingContext, context), context);
      MultipleRecordsFetchClient holdingsRecordFetchClient = createHoldingsRecordsFetchClient(holdingsStorageClient);

      LOGGER.info("updateHoldingsForInstance:: Fetching {} holdings records to be moved.",
        holdingsRecordsIdsToUpdate.size());

      holdingsRecordFetchClient.find(holdingsRecordsIdsToUpdate, MoveApiUtil::fetchByIdCql)
        .thenAccept(jsons -> {
          LOGGER.info(
            "updateHoldingsForInstance:: Found {} of {} holdings records. Preparing to update their instanceId to {}.",
            jsons.size(), holdingsRecordsIdsToUpdate.size(), instance.getId());

          if (jsons.isEmpty() && !holdingsRecordsIdsToUpdate.isEmpty()) {
            LOGGER.warn(
              "updateHoldingsForInstance:: None of the requested holdings records [{}] were found for moving.",
              holdingsRecordsIdsToUpdate);
          }

          List<HoldingsRecord> holdingsRecordsToUpdate = updateInstanceIdForHoldings(instance.getId(), jsons);
          updateHoldings(routingContext, context, holdingsRecordsIdsToUpdate, holdingsRecordsToUpdate);
        })
        .exceptionally(e -> {
          LOGGER.error("updateHoldingsForInstance:: Failed to fetch holdings records for moving. IDs: {}",
            holdingsRecordsIdsToUpdate, e);
          ServerErrorResponse.internalError(routingContext.response(), e);
          return null;
        });
    } catch (Exception e) {
      LOGGER.error("updateHoldingsForInstance:: Failed to initialize storage clients for updating holdings.", e);
      ServerErrorResponse.internalError(routingContext.response(), e);
    }
  }

  private CompletableFuture<Instance> findInstanceInConsortium(Context context, String toInstanceId) {
    LOGGER.info("findInstanceInConsortium:: Attempting to find instance {} in consortium.", toInstanceId);
    return consortiumService.getConsortiumConfiguration(context)
      .toCompletionStage().toCompletableFuture()
      .thenCompose(consortiumConfig -> {
        if (consortiumConfig.isPresent()) {
          LOGGER.info("findInstanceInConsortium:: Tenant is part of consortium '{}'. Searching in central tenant '{}'.",
            consortiumConfig.get().consortiumId(), consortiumConfig.get().centralTenantId());

          Context centralTenantContext = constructContext(
            consortiumConfig.get().centralTenantId(), context.getToken(), context.getOkapiLocation(),
            context.getUserId(), context.getRequestId()
          );
          return storage.getInstanceCollection(centralTenantContext).findById(toInstanceId)
            .thenApply(sharedInstance -> {
              if (sharedInstance != null) {
                LOGGER.info("findInstanceInConsortium:: Successfully found shared instance {} in central tenant.",
                  toInstanceId);
              } else {
                LOGGER.info("findInstanceInConsortium:: Instance {} was not found in central tenant.", toInstanceId);
              }
              return sharedInstance;
            });
        }
        LOGGER.info(
          "findInstanceInConsortium:: Tenant is not part of a consortium. Skipping search in central tenant.");
        return CompletableFuture.completedFuture(null);
      });
  }

  /**
   * Fetches the highest numeric {@code order} among items of the given holdings record using a single
   * limit-1 query. The {@code /number} CQL modifier is required: the storage index on {@code order} is
   * untyped, so without it values would be sorted as text ("9" before "10").
   *
   * <p>Note: the read is not atomic with the subsequent update, so two concurrent moves into the same holdings
   * record may be assigned the same order values. This is an accepted limitation.
   *
   * @param itemsStorageClient client for /item-storage/items
   * @param holdingsRecordId   the id of the holdings record
   * @return the highest order, or 0 when the holdings record has no items (or the item has no order)
   */
  private CompletableFuture<Integer> fetchMaxOrder(CollectionResourceClient itemsStorageClient,
                                                   String holdingsRecordId) {
    var cql = CqlQuery.exactMatch("holdingsRecordId", holdingsRecordId) + " sortBy order/number/sort.descending";
    return itemsStorageClient.getMany(cql, 1, 0)
      .thenApply(response -> {
        if (response.statusCode() != 200) {
          throw new ExternalResourceFetchException(response);
        }
        var topItems = response.getJson().getJsonArray(MoveApiUtil.ITEMS_PROPERTY);
        if (topItems == null || topItems.isEmpty()) {
          return 0;
        }
        return Optional.ofNullable(topItems.getJsonObject(0).getInteger(Item.ORDER_KEY)).orElse(0);
      });
  }

  /**
   * Updates the holdingId and assigns an explicit order to each item, continuing after the highest order
   * currently present in the target holdings record. An explicit order bypasses the storage sequence
   * ({@code item_order_tracker}), which is never decremented when items leave a holdings record and would
   * otherwise produce inflated values after items are moved back and forth.
   * Moved items keep their relative order; items without order go last.
   *
   * @param toHoldingsRecordId the id of the holdings record to which items will be moved
   * @param jsons              the list of items in JSON format to be updated
   * @param currentMaxOrder    the highest order currently used in the target holdings record (0 if none)
   * @return a list of Item objects with updated holdingId and order fields
   */
  private List<Item> updateItemFields(String toHoldingsRecordId, List<JsonObject> jsons, int currentMaxOrder) {
    var sortedJsons = jsons.stream()
      .sorted(Comparator.comparing(json -> json.getInteger(Item.ORDER_KEY),
        Comparator.nullsLast(Comparator.naturalOrder())))
      .toList();

    return IntStream.range(0, sortedJsons.size())
      .mapToObj(i -> ItemUtil.fromStoredItemRepresentation(sortedJsons.get(i))
        .withHoldingId(toHoldingsRecordId)
        .withOrder(currentMaxOrder + i + 1))
      .toList();
  }

  private void updateItems(RoutingContext routingContext, WebContext context, List<String> idsToUpdate,
                           List<Item> itemsToUpdate) {
    ItemCollection storageItemCollection = storage.getItemCollection(context);

    List<CompletableFuture<Item>> updates = itemsToUpdate.stream()
      .map(storageItemCollection::update)
      .toList();

    CompletableFuture.allOf(updates.toArray(new CompletableFuture[0]))
      .handle((v, throwable) -> updates.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(Item::getId)
        .toList())
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }

  private List<HoldingsRecord> updateInstanceIdForHoldings(String toInstanceId, List<JsonObject> jsons) {
    jsons.forEach(MoveApiUtil::removeExtraRedundantFields);

    return jsons.stream()
      .map(json -> json.mapTo(HoldingsRecord.class))
      .map(holding -> holding.withInstanceId(toInstanceId))
      .toList();
  }

  private void updateHoldings(RoutingContext routingContext, WebContext context, List<String> idsToUpdate,
                              List<HoldingsRecord> holdingsToUpdate) {
    HoldingsRecordCollection storageHoldingsRecordsCollection = storage.getHoldingsRecordCollection(context);

    List<CompletableFuture<HoldingsRecord>> updateFutures = holdingsToUpdate.stream()
      .map(storageHoldingsRecordsCollection::update)
      .toList();

    CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0]))
      .handle((v, throwable) -> updateFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(HoldingsRecord::getId)
        .toList())
      .thenAccept(updatedIds -> respond(routingContext, idsToUpdate, updatedIds));
  }
}
