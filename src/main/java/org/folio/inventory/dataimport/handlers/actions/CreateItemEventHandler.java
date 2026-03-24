package org.folio.inventory.dataimport.handlers.actions;

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
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

public class CreateItemEventHandler implements EventHandler {

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String PAYLOAD_DATA_HAS_NO_HOLDINGS = "Failed to extract holdingsRecord from payload";
  private static final String HOLDING_PERMANENT_LOCATION_SHOULD_NOT_BE_BLANK = "Holding permanent location id should not be blank";
  private static final String PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG = "Failed to extract poLineId from poLine entity";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE_MSG = "All Items should have the same material type, during the creation of open order";
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

  private static final Logger LOGGER = LogManager.getLogger(CreateItemEventHandler.class);

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
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_ITEM_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || isBlank(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()))) {
        LOGGER.warn("handle:: " + PAYLOAD_HAS_NO_DATA_MSG + " jobExecutionId: {}", jobExecutionId);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      String recordId = payloadContext.get(RECORD_ID_HEADER);
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.warn("handle:: " + ACTION_HAS_NO_MAPPING_MSG + " jobExecutionId: {} recordId: {}", jobExecutionId, recordId);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
      dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().getFirst());
      dataImportEventPayload.getContext().put(ITEM.value(), new JsonArray().encode());

      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      LOGGER.info("handle:: Create items with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

      Future<RecordToEntity> recordToItemFuture = idStorageService.store(recordId, UUID.randomUUID().toString(), dataImportEventPayload.getTenant());
      recordToItemFuture.onSuccess(res -> {
        String deduplicationItemId = res.getEntityId();
        Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(),
          payloadContext.get(PAYLOAD_USER_ID), payloadContext.get(OKAPI_REQUEST_ID));
        ItemCollection itemCollection = storage.getItemCollection(context);

        mappingMetadataCache.get(jobExecutionId, context)
          .map(parametersOptional -> parametersOptional
            .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId,
              recordId, chunkId))))
          .map(mappingMetadataDto -> {
            MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
            MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));
            return processMappingResult(dataImportEventPayload, deduplicationItemId);
          })
          .compose(mappedItemList -> {
            LOGGER.trace(format("handle:: Mapped items: %s", mappedItemList.encode()));
            Promise<List<Item>> createMultipleItemsPromise = Promise.promise();
            List<PartialError> multipleItemsCreateErrors = new ArrayList<>();
            List<Future<?>> createItemsFutures = new ArrayList<>();
            List<Item> createdItems = new ArrayList<>();

            mappedItemList.forEach(e -> {
              JsonObject itemAsJson = getItemFromJson((JsonObject) e);
              Promise<Item> createItemPromise = Promise.promise();
              processSingleItem(jobExecutionId, recordId, chunkId, itemCollection, itemAsJson)
                .onSuccess(item -> {
                  createdItems.add(item);
                  createItemPromise.complete();
                })
                .onFailure(cause -> {
                  String itemId = itemAsJson.getString(ITEM_ID_FIELD) != null ? itemAsJson.getString(ITEM_ID_FIELD) : BLANK;
                  String holdingId = itemAsJson.getString(HOLDINGS_RECORD_ID_FIELD) != null ? itemAsJson.getString(HOLDINGS_RECORD_ID_FIELD) : BLANK;
                  PartialError partialError = new PartialError(itemId, cause.getMessage());
                  partialError.setHoldingId(holdingId);
                  multipleItemsCreateErrors.add(partialError);
                  if (cause instanceof DuplicateEventException) {
                    createItemPromise.fail(cause);
                  } else {
                    createItemPromise.complete();
                  }
                });

              createItemsFutures.add(createItemPromise.future());
            });

            Future.all(createItemsFutures).onComplete(ar -> {
              if (payloadContext.containsKey(ERRORS) || !multipleItemsCreateErrors.isEmpty()) {
                payloadContext.put(ERRORS, Json.encode(multipleItemsCreateErrors));
              }
              if (ar.succeeded()) {
                String multipleItemsCreateErrorsAsStringJson = Json.encode(multipleItemsCreateErrors);
                if (!createdItems.isEmpty()) {
                  payloadContext.put(ERRORS, multipleItemsCreateErrorsAsStringJson);
                  createMultipleItemsPromise.complete(createdItems);
                } else {
                  createMultipleItemsPromise.fail(multipleItemsCreateErrorsAsStringJson);
                }
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
                LOGGER.warn("handle:: Error creating inventory Item by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
                  recordId, chunkId, ar.cause());
              }
              future.completeExceptionally(ar.cause());
            }
          });
      }).onFailure(failure -> {
        LOGGER.warn("handle:: Error creating inventory recordId and itemId relationship by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId, recordId,
          chunkId, failure);
        future.completeExceptionally(failure);
      });
    } catch (Exception e) {
      LOGGER.warn("handle:: Error creating inventory Item", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Item> processSingleItem(String jobExecutionId, String recordId, String chunkId, ItemCollection itemCollection, JsonObject mappedItemJson) {
    List<String> errors = validateItem(mappedItemJson, requiredFields);
    LOGGER.debug(format("processSingleItem:: Trying to create item with id: %s", mappedItemJson.getString("id")));
    if (!errors.isEmpty()) {
      String msg = format("Mapped Item is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", errors,
        jobExecutionId, recordId, chunkId);
      LOGGER.warn("processSingleItem:: " + msg);
      return Future.failedFuture(msg);
    }
    Item mappedItem = ItemUtil.jsonToItem(mappedItemJson);
    return isItemBarcodeUnique(mappedItemJson.getString("barcode"), itemCollection)
      .compose(isUnique -> isUnique
        ? addItem(mappedItem, itemCollection)
        : Future.failedFuture(format("Barcode must be unique, %s is already assigned to another item", mappedItemJson.getString("barcode"))));
  }

  private JsonArray processMappingResult(DataImportEventPayload dataImportEventPayload, String deduplicationItemId) {
    JsonArray items = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    JsonArray mappedItems = new JsonArray();
    JsonArray holdingsIdentifiers = getHoldingsIdentifiers(dataImportEventPayload);
    String holdingsAsString = dataImportEventPayload.getContext().get(EntityType.HOLDINGS.value());

    for (int i = 0; i < items.size(); i++) {
      JsonObject itemAsJson = getItemFromJson(items.getJsonObject(i));
      String holdingPermanentLocation = holdingsIdentifiers.size() > i ? holdingsIdentifiers.getString(i) : null;
      fillPoLineIdIfNecessary(dataImportEventPayload, itemAsJson);
      if (fillHoldingsRecordIdIfNecessary(itemAsJson, holdingsAsString, holdingPermanentLocation)) {
        mappedItems.add(itemAsJson);
      }
    }

    for (int i = 0; i < mappedItems.size(); i++) {
      JsonObject itemAsJson = getItemFromJson(mappedItems.getJsonObject(i));
      itemAsJson.put(ITEM_ID_FIELD, (i == 0) ? deduplicationItemId : UUID.randomUUID().toString());
    }

    if (dataImportEventPayload.getContext().containsKey(EntityType.PO_LINE.value())) {
      validateItemsToContainSameMaterialType(mappedItems);
    }

    return mappedItems;
  }

  private void validateItemsToContainSameMaterialType(JsonArray mappedItems) {
    List<String> materialTypes = mappedItems.stream().map(o -> {
      JsonObject materialType = getItemFromJson((JsonObject) o).getJsonObject("materialType");
      return materialType == null ? null : materialType.getString("id");
    }).toList();

    if (materialTypes.stream().distinct().count() != 1) {
      LOGGER.warn("validateItemsToContainSameMaterialType:: " + ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE_MSG);
      throw new EventProcessingException(ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE_MSG);
    }
  }

  private static JsonArray getHoldingsIdentifiers(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getContext().containsKey(HOLDING_IDENTIFIERS)) {
      return new JsonArray(dataImportEventPayload.getContext().remove(HOLDING_IDENTIFIERS));
    }
    return new JsonArray();
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
          LOGGER.warn("fillPoLineIdIfNecessary:: " + PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG);
          throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_PO_LINE_ID_MSG);
        }

        itemAsJson.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, poLineId);
      }
    }
  }

  private boolean fillHoldingsRecordIdIfNecessary(JsonObject itemAsJson, String holdingsAsString, String holdingPermanentLocation) {
    if (isBlank(itemAsJson.getString(HOLDINGS_RECORD_ID_FIELD))) {
      String holdingId;
      if (StringUtils.isNotEmpty(holdingsAsString)) {
        holdingId = getHoldingByPermanentLocation(holdingsAsString, holdingPermanentLocation);
      } else {
        LOGGER.warn("fillHoldingsRecordIdIfNecessary:: " + PAYLOAD_DATA_HAS_NO_HOLDINGS);
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_HOLDINGS);
      }
      if (holdingId == null) return false;
      itemAsJson.put(HOLDINGS_RECORD_ID_FIELD, holdingId);
    }
    return true;
  }

  private static String getHoldingByPermanentLocation(String holdingsAsString, String holdingPermanentLocation) {
    JsonArray holdingsRecords = new JsonArray(holdingsAsString);
    if (holdingsRecords.size() == 1) {
      return holdingsRecords.getJsonObject(0).getString(HOLDING_ID_FIELD);
    }

    if (holdingPermanentLocation == null) {
      LOGGER.debug("getHoldingByPermanentLocation:: " + HOLDING_PERMANENT_LOCATION_SHOULD_NOT_BE_BLANK);
      return null;
    }

    Optional<JsonObject> correspondingHolding = holdingsRecords.stream()
      .map(e -> (JsonObject) e)
      .filter(holding -> StringUtils.equals(holding.getString(HOLDING_PERMANENT_LOCATION_ID), holdingPermanentLocation)).findFirst();

    return correspondingHolding.map(entries -> entries.getString(HOLDING_ID_FIELD)).orElse(null);
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
      LOGGER.warn(format("isItemBarcodeUnique:: Error to find items by barcode '%s'", barcode), e);
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

    if (LOGGER.isTraceEnabled()) {
      notes.forEach(note -> LOGGER.trace("addItem:: circulation note with id : {} added to item with itemId: {}", note.getId(), item.getId()));
    }

    itemCollection.add(item.withCirculationNotes(notes), success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOGGER.info("addItem:: Duplicated event received by ItemId: {}. Ignoring...", item.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Item id: %s", item.getId())));
        } else {
          LOGGER.warn(format("addItem:: Error posting Item cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  public static JsonObject getItemFromJson(JsonObject itemAsJson) {
    if (itemAsJson.getJsonObject(ITEM_PATH_FIELD) != null) {
      return itemAsJson.getJsonObject(ITEM_PATH_FIELD);
    }
    return itemAsJson;
  }
}
