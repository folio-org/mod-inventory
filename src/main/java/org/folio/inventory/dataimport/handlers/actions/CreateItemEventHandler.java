package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonHelper;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.EntityType;

import java.io.UnsupportedEncodingException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateItemEventHandler implements EventHandler {

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String PAYLOAD_DATA_HAS_NO_HOLDINGS = "Failed to extract holdingsRecord from payload";
  private static final String PAYLOAD_DATA_HAS_NO_HOLDING_WITH_CORRESPONDING_IDENTIFIER = "Holdings record with identifier: %s does not exist";
  private static final String HOLDING_PERMANENT_LOCATION_SHOULD_NOT_BE_BLANK = "Holding permanent location id should not be blank";
  private static final String PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG = "Failed to extract poLineId from poLine entity";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create an Item requires a mapping profile";
  public static final String HOLDINGS_RECORD_ID_FIELD = "holdingsRecordId";
  public static final String ITEM_PATH_FIELD = "item";
  public static final String HOLDING_ID_FIELD = "id";
  public static final String ITEM_ID_FIELD = "id";
  public static final String PO_LINE_ID_FIELD = "id";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String ERRORS = "ERRORS";
  private static final String HOLDING_PERMANENT_LOCATION_ID = "permanentLocationId";
  private static final String HOLDING_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
  private static final String BLANK = "";
  private static final Map<String, String> validNotes = Map.of(
    "Check in note", "Check in",
    "Check out note", "Check out");

  private static final Logger LOG = LogManager.getLogger(CreateItemEventHandler.class);

  private final DateTimeFormatter dateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  private final List<String> requiredFields = Arrays.asList("status.name", "materialType.id", "permanentLoanType.id", "holdingsRecordId");

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;
  private IdStorageService idStorageService;

  private OrderHelperService orderHelperService;

  public CreateItemEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService,
                                OrderHelperService orderHelperService) {
    this.orderHelperService = orderHelperService;
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
    this.idStorageService = idStorageService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOG, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_ITEM_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || isBlank(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOG.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
      dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
      dataImportEventPayload.getContext().put(ITEM.value(), new JsonObject().encode());

      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      LOG.info("Create item with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

      Future<RecordToEntity> recordToItemFuture = idStorageService.store(recordId, UUID.randomUUID().toString(), dataImportEventPayload.getTenant());
      recordToItemFuture.onSuccess(res -> {
        String deduplicationItemId = res.getEntityId();
        Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
        ItemCollection itemCollection = storage.getItemCollection(context);

        mappingMetadataCache.get(jobExecutionId, context)
          .map(parametersOptional -> parametersOptional
            .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId,
              recordId, chunkId))))
          .map(mappingMetadataDto -> {
            MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
            MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

            // TO DELETE
            JsonObject itemsJson = new JsonObject(payloadContext.get(ITEM.value()));
            JsonArray items;
            if (itemsJson.getJsonObject(ITEM_PATH_FIELD) == null) {
              items = new JsonArray(List.of(itemsJson));
            } else {
              items = new JsonArray(List.of(itemsJson.getJsonObject(ITEM_PATH_FIELD)));
            }
            payloadContext.put(ITEM.value(), items.toString());
            // TO DELETE

            return processMappingResult(dataImportEventPayload, deduplicationItemId);
          })
          .compose(mappedItemList -> {
            Promise<List<Item>> createMultipleItemsPromise = Promise.promise();
            List<PartialError> multipleItemsCreateErrors = new ArrayList<>();
            List<Future> createItemsFutures = new ArrayList<>();
            List<Item> createdItems = new ArrayList<>();

            mappedItemList.forEach(e -> {
              JsonObject itemAsJson = (JsonObject) e;
              Promise<Item> createItemPromise = Promise.promise();
              processSingleItem(jobExecutionId, recordId, chunkId, itemCollection, itemAsJson)
                .onSuccess(item -> {
                  createdItems.add(item);
                  createItemPromise.complete();
                })
                .onFailure(cause -> {
                  String itemId = itemAsJson.getString(ITEM_ID_FIELD) != null ? itemAsJson.getString(ITEM_ID_FIELD) : BLANK;
                  multipleItemsCreateErrors.add(new PartialError(itemId, cause.getMessage()));
                  if (cause instanceof DuplicateEventException) {
                    createItemPromise.fail(cause);
                  } else {
                    createItemPromise.complete();
                  }
                });

              createItemsFutures.add(createItemPromise.future());
            });

            CompositeFuture.all(createItemsFutures).onComplete(ar -> {
              payloadContext.put(ERRORS, Json.encode(multipleItemsCreateErrors));
              if (ar.succeeded()) {
                createMultipleItemsPromise.complete(createdItems);
              } else {
                createMultipleItemsPromise.fail(ar.cause());
              }
            });
            return createMultipleItemsPromise.future();
          })
          .onComplete(ar -> {
            if (ar.succeeded()) {
              dataImportEventPayload.getContext().put(ITEM.value(), Json.encode(ar.result()));
              orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload, DI_INVENTORY_ITEM_CREATED, context)
                .onComplete(result -> future.complete(dataImportEventPayload)
                );
            } else {
              if (!(ar.cause() instanceof DuplicateEventException)) {
                LOG.error("Error creating inventory Item by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
                  recordId, chunkId, ar.cause());
              }
              future.completeExceptionally(ar.cause());
            }
          });
      }).onFailure(failure -> {
        LOG.error("Error creating inventory recordId and itemId relationship by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId, recordId,
          chunkId, failure);
        future.completeExceptionally(failure);
      });
    } catch (Exception e) {
      LOG.error("Error creating inventory Item", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Item> processSingleItem(String jobExecutionId, String recordId, String chunkId, ItemCollection itemCollection, JsonObject mappedItemJson) {
    List<String> errors = validateItem(mappedItemJson, requiredFields);
    if (!errors.isEmpty()) {
      String msg = format("Mapped Item is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", errors,
        jobExecutionId, recordId, chunkId);
      LOG.warn(msg);
      return Future.failedFuture(msg);
    }
    Item mappedItem = ItemUtil.jsonToItem(mappedItemJson);
    return isItemBarcodeUnique(mappedItemJson.getString("barcode"), itemCollection)
      .compose(isUnique -> isUnique
        ? addItem(mappedItem, itemCollection)
        : Future.failedFuture(format("Barcode must be unique, %s is already assigned to another item", mappedItemJson.getString("barcode"))));
  }

  private JsonArray processMappingResult(DataImportEventPayload dataImportEventPayload, String deduplicationItemId) {
    JsonArray itemsList = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    JsonArray holdingsIdentifiers = new JsonArray(dataImportEventPayload.getContext().get(HOLDING_IDENTIFIERS));
    String holdingsAsString = dataImportEventPayload.getContext().get(EntityType.HOLDINGS.value());

    for (int i = 0; i < itemsList.size(); i++) {
      JsonObject itemAsJson = itemsList.getJsonObject(i);
      String holdingPermanentLocation = holdingsIdentifiers.size() > i ? holdingsIdentifiers.getString(i) : null;
      fillPoLineIdIfNecessary(dataImportEventPayload, itemAsJson);
      fillHoldingsRecordIdIfNecessary(itemAsJson, holdingsAsString, holdingPermanentLocation);
      itemAsJson.put(ITEM_ID_FIELD, (i == 0) ? deduplicationItemId : UUID.randomUUID().toString());
    }
    return itemsList;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == ITEM;
    }
    return false;
  }

  private void fillPoLineIdIfNecessary(DataImportEventPayload dataImportEventPayload, JsonObject itemAsJson) {
    if (isBlank(itemAsJson.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))) {
      String poLineAsString = dataImportEventPayload.getContext().get(EntityType.PO_LINE.value());
      if (StringUtils.isNotEmpty(poLineAsString)) {
        JsonObject poLineAsJson = new JsonObject(poLineAsString);
        String poLineId = poLineAsJson.getString(PO_LINE_ID_FIELD);

        if (isBlank(poLineId)) {
          LOG.error(PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG);
          throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG);
        }

        itemAsJson.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, poLineId);
      }
    }
  }

  private void fillHoldingsRecordIdIfNecessary(JsonObject itemAsJson, String holdingsAsString, String holdingPermanentLocation) {
    if (isBlank(itemAsJson.getString(HOLDINGS_RECORD_ID_FIELD))) {
      String holdingId;
      if (StringUtils.isNotEmpty(holdingsAsString)) {
        holdingId = getHoldingByPermanentLocation(holdingsAsString, holdingPermanentLocation);
      } else {
        LOG.warn(PAYLOAD_DATA_HAS_NO_HOLDINGS);
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_HOLDINGS);
      }
      itemAsJson.put(HOLDINGS_RECORD_ID_FIELD, holdingId);
    }
  }

  private static String getHoldingByPermanentLocation(String holdingsAsString, String holdingPermanentLocation) {
    JsonArray holdingsRecords = new JsonArray(holdingsAsString);
    if (holdingsRecords.size() == 1) {
      return holdingsRecords.getJsonObject(0).getString(HOLDING_ID_FIELD);
    }

    if (holdingPermanentLocation == null) {
      LOG.warn(HOLDING_PERMANENT_LOCATION_SHOULD_NOT_BE_BLANK);
      throw new EventProcessingException(HOLDING_PERMANENT_LOCATION_SHOULD_NOT_BE_BLANK);
    }

    Optional<JsonObject> correspondingHolding = holdingsRecords.stream()
      .map(e -> new JsonObject((String) e))
      .filter(holding -> StringUtils.equals(holding.getString(HOLDING_PERMANENT_LOCATION_ID), holdingPermanentLocation)).findFirst();

    return correspondingHolding.orElseThrow(() -> {
      String errorMsg = format(PAYLOAD_DATA_HAS_NO_HOLDING_WITH_CORRESPONDING_IDENTIFIER, holdingPermanentLocation);
      LOG.warn(errorMsg);
      throw new EventProcessingException(errorMsg);
    }).getString(HOLDING_ID_FIELD);
  }

  private void validateStatusName(JsonObject itemAsJson, List<String> errors) {
    String statusName = JsonHelper.getNestedProperty(itemAsJson, "status", "name");
    if (StringUtils.isNotBlank(statusName) && !ItemStatusName.isStatusCorrect(statusName)) {
      errors.add(format("Invalid status specified '%s'", statusName));
    }
  }

  private List<String> validateItem(JsonObject itemAsJson, List<String> requiredFields) {
    List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(itemAsJson, requiredFields);
    validateStatusName(itemAsJson, errors);
    return errors;
  }

  private Future<Boolean> isItemBarcodeUnique(String barcode, ItemCollection itemCollection) {

    if (isEmpty(barcode)) {
      return Future.succeededFuture(Boolean.TRUE);
    }

    Promise<Boolean> promise = Promise.promise();
    try {
      itemCollection.findByCql(CqlHelper.barcodeIs(barcode), PagingParameters.defaults(),
        findResult -> promise.complete(findResult.getResult().records.isEmpty()),
        failure -> promise.fail(failure.getReason()));
    } catch (UnsupportedEncodingException e) {
      LOG.error(format("Error to find items by barcode '%s'", barcode), e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<Item> addItem(Item item, ItemCollection itemCollection) {
    Promise<Item> promise = Promise.promise();
    List<CirculationNote> notes = item.getCirculationNotes()
      .stream()
      .map(note -> note.withId(UUID.randomUUID().toString()))
      .map(note -> note.withSource(null))
      .map(note -> note.withDate(dateTimeFormatter.format(ZonedDateTime.now())))
      .map(note -> note.withNoteType(validNotes.getOrDefault(note.getNoteType(), note.getNoteType())))
      .collect(Collectors.toList());

    if (LOG.isTraceEnabled()) {
      notes.forEach(note -> LOG.trace("addItem:: circulation note with id : {} added to item with itemId: {}", note.getId(), item.getId()));
    }

    itemCollection.add(item.withCirculationNotes(notes), success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOG.info("addItem:: Duplicated event received by ItemId: {}. Ignoring...", item.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Item id: %s", item.getId())));
        } else {
          LOG.warn(format("addItem:: Error posting Item cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }
}
