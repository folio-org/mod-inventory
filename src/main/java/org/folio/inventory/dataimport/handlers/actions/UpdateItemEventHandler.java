package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonHelper;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;

import java.io.UnsupportedEncodingException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class UpdateItemEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger(UpdateItemEventHandler.class);

  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Item requires a mapping profile";
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data or ITEM to update";
  private static final String STATUS_UPDATE_ERROR_MSG = "Could not change item status '%s' to '%s'";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'.RecordId: '%s', chunkId: '%s'";
  private static final String ITEM_PATH_FIELD = "item";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final Set<String> PROTECTED_STATUSES_FROM_UPDATE = new HashSet<>(Arrays.asList("Aged to lost", "Awaiting delivery", "Awaiting pickup", "Checked out", "Claimed returned", "Declared lost", "Paged", "Recently returned"));

  private final List<String> requiredFields = Arrays.asList("status.name", "materialType.id", "permanentLoanType.id", "holdingsRecordId");
  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  public UpdateItemEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_ITEM_UPDATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))
        || isBlank(payloadContext.get(ITEM.value()))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOG.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      AtomicBoolean isProtectedStatusChanged = new AtomicBoolean();
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();

      mappingMetadataCache.get(jobExecutionId, context)
        .map(parametersOptional -> parametersOptional
          .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId,
            dataImportEventPayload.getContext().get(RECORD_ID_HEADER),dataImportEventPayload.getContext().get(CHUNK_ID_HEADER)))))
        .compose(mappingMetadataDto -> {
          JsonObject itemAsJson = new JsonObject(payloadContext.get(ITEM.value()));
          String oldItemStatus = itemAsJson.getJsonObject(STATUS_KEY).getString("name");
          preparePayloadForMappingManager(dataImportEventPayload);
          MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
          MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

          JsonObject mappedItemAsJson = new JsonObject(payloadContext.get(ITEM.value()));
          mappedItemAsJson = mappedItemAsJson.containsKey(ITEM_PATH_FIELD) ? mappedItemAsJson.getJsonObject(ITEM_PATH_FIELD) : mappedItemAsJson;

          List<String> errors = validateItem(mappedItemAsJson, requiredFields);
          if (!errors.isEmpty()) {
            String msg = format("Mapped Item is invalid: %s", errors.toString());
            LOG.error(msg);
            return Future.failedFuture(msg);
          }

          String newItemStatus = mappedItemAsJson.getJsonObject(STATUS_KEY).getString("name");
          isProtectedStatusChanged.set(isProtectedStatusChanged(oldItemStatus, newItemStatus));
          if (isProtectedStatusChanged.get()) {
            mappedItemAsJson.getJsonObject(STATUS_KEY).put("name", oldItemStatus);
          }

          ItemCollection itemCollection = storage.getItemCollection(context);
          Item itemToUpdate = ItemUtil.jsonToItem(mappedItemAsJson);
          return verifyItemBarcodeUniqueness(itemToUpdate, itemCollection)
            .compose(v -> updateItem(itemToUpdate, itemCollection))
            .onSuccess(updatedItem -> {
              if (isProtectedStatusChanged.get()) {
                String msg = String.format(STATUS_UPDATE_ERROR_MSG, oldItemStatus, newItemStatus);
                LOG.warn(msg);
                dataImportEventPayload.getContext().put(ITEM.value(), ItemUtil.mapToJson(updatedItem).encode());
                future.completeExceptionally(new EventProcessingException(msg));
              } else {
                dataImportEventPayload.getContext().put(ITEM.value(), ItemUtil.mapToJson(updatedItem).encode());
                future.complete(dataImportEventPayload);
              }
            });
        })
        .onFailure(e -> {
          LOG.error("Failed to update inventory Item by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
            dataImportEventPayload.getContext().get(RECORD_ID_HEADER), dataImportEventPayload.getContext().get(CHUNK_ID_HEADER), e);
          future.completeExceptionally(e);
        });
    } catch (Exception e) {
      LOG.error("Error updating inventory Item", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private boolean isProtectedStatusChanged(String oldItemStatus, String newItemStatus) {
    return PROTECTED_STATUSES_FROM_UPDATE.contains(oldItemStatus) && !oldItemStatus.equals(newItemStatus);
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.ITEM;
    }
    return false;
  }

  private void preparePayloadForMappingManager(DataImportEventPayload dataImportEventPayload) {
    JsonObject oldItemJson = new JsonObject(dataImportEventPayload.getContext().get(ITEM.value()));
    dataImportEventPayload.getContext().put(ActionProfile.FolioRecord.ITEM.value(), new JsonObject().put(ITEM_PATH_FIELD, oldItemJson).encode());
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private List<String> validateItem(JsonObject itemAsJson, List<String> requiredFields) {
    List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(itemAsJson, requiredFields);
    validateStatusName(itemAsJson, errors);
    return errors;
  }

  private void validateStatusName(JsonObject itemAsJson, List<String> errors) {
    String statusName = JsonHelper.getNestedProperty(itemAsJson, STATUS_KEY, "name");
    if (StringUtils.isNotBlank(statusName) && !ItemStatusName.isStatusCorrect(statusName)) {
      errors.add(format("Invalid status specified '%s'", statusName));
    }
  }

  private Future<Boolean> verifyItemBarcodeUniqueness(Item item, ItemCollection itemCollection) {

    if (isEmpty(item.getBarcode())) {
      return Future.succeededFuture(true);
    }

    Promise<Boolean> promise = Promise.promise();
    try {
      itemCollection.findByCql(CqlHelper.barcodeIs(item.getBarcode()) + " AND id <> " + item.id, PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult().records.isEmpty()) {
            promise.complete(findResult.getResult().records.isEmpty());
          } else {
            promise.fail(format("Barcode must be unique, %s is already assigned to another item", item.getBarcode()));
          }
        },
        failure -> promise.fail(failure.getReason()));
    } catch (UnsupportedEncodingException e) {
      String msg = format("Failed to find items by barcode '%s'", item.getBarcode());
      LOG.error(msg, e);
      promise.fail(msg);
    }
    return promise.future();
  }

  private Future<Item> updateItem(Item item, ItemCollection itemCollection) {
    Promise<Item> promise = Promise.promise();
    item.getCirculationNotes().forEach(note -> note
      .withId(UUID.randomUUID().toString())
      .withSource(null)
      .withDate(dateTimeFormatter.format(ZonedDateTime.now())));

    itemCollection.update(item).whenComplete((updatedItem, e) -> {
      if (e != null) {
        promise.fail(e);
        return;
      }
      promise.complete(updatedItem);
    });
    return promise.future();
  }
}
