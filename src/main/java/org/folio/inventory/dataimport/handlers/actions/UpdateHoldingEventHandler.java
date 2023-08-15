package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.ItemUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import com.fasterxml.jackson.core.JsonProcessingException;

public class UpdateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateHoldingEventHandler.class);

  private static final String UPDATE_HOLDING_ERROR_MESSAGE = "Can`t update  holding by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s'";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t update Holding entity: context or Holding-entity are empty or doesn`t exist!";
  private static final String EMPTY_REQUIRED_FIELDS_ERROR_MESSAGE = "Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!";
  private static final String MAPPING_METADATA_NOT_FOUND_MESSAGE = "MappingMetadata snapshot was not found by jobExecutionId '%s'. Record: '%s', chunkId: '%s' ";
  private static final String HOLDINGS_PATH_FIELD = "holdings";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update a Holding entity has no a mapping profile";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String ITEM_ID_HEADER = "id";
  static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_HOLDING_PROPERTY = "CURRENT_HOLDING";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String CANNOT_UPDATE_HOLDING_ERROR_MESSAGE = "Error updating Holding by holdingId '%s' and jobExecution '%s' recordId '%s' chunkId '%s' - %s, status code %s";
  private static final String CANNOT_GET_ACTUAL_ITEM_MESSAGE = "Cannot get actual Item after successfully updating holdings, by ITEM id: '%s' - '%s', status code '%s'";
  private static final String BLANK = "";
  private static final String ERRORS = "ERRORS";
  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;
  boolean isPayloadConstructed = false;

  public UpdateHoldingEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_UPDATED.value());

      if (dataImportEventPayload.getContext() == null
        || isEmpty(dataImportEventPayload.getContext().get(HOLDINGS.value()))
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))) {
        LOGGER.warn("handle:: Can`t update Holding entity context: {}", dataImportEventPayload.getContext());
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.warn("handle:: " + ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      LOGGER.info("handle:: Processing UpdateHoldingEventHandler starting with jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());
      List<PartialError> errors = new ArrayList<>();

      validateRequiredHoldingsFields(dataImportEventPayload, errors);
      if (!errors.isEmpty()) {
        dataImportEventPayload.getContext().put(ERRORS, Json.encode(errors));
        future.complete(dataImportEventPayload);
        return future;
      }
      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      LOGGER.info("handle:: Update holding with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

      mappingMetadataCache.get(jobExecutionId, context)
        .map(parametersOptional -> parametersOptional
          .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MESSAGE, jobExecutionId,
            recordId, chunkId))))
        .onSuccess(mappingMetadataDto -> {
          prepareEvent(dataImportEventPayload);
          MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
          MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

          List<HoldingsRecord> updatedHoldingsRecord = new ArrayList<>();
          List<Future> updatedHoldingsRecordFutures = new ArrayList<>();
          isPayloadConstructed = false;
          convertHoldings(dataImportEventPayload);
          List<HoldingsRecord> list = List.of(Json.decodeValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
          LOGGER.trace(format("handle:: Mapped holding: %s", Json.decodeValue(dataImportEventPayload.getContext().get(HOLDINGS.value()))));
          HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
          List<HoldingsRecord> expiredHoldings = new ArrayList<>();
          for (HoldingsRecord holding : list) {
            Promise<Void> updatePromise = Promise.promise();
            updatedHoldingsRecordFutures.add(updatePromise.future());
            holdingsRecordCollection.update(holding,
              success -> {
                LOGGER.info(format("handle:: Successfully updated holdings with id: %s", holding.getId()));
                updatedHoldingsRecord.add(holding);
                constructDataImportEventPayload(updatePromise, dataImportEventPayload, list, context, errors);
              },
              failure -> {
                if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
                  expiredHoldings.add(holding);
                } else {
                  errors.add(new PartialError(holding.getId() != null ? holding.getId() : BLANK, failure.getReason()));
                  dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
                  LOGGER.warn("handle:: " + format(CANNOT_UPDATE_HOLDING_ERROR_MESSAGE, holding.getId(), jobExecutionId, recordId, chunkId, failure.getReason(), failure.getStatusCode()));
                }
                updatePromise.complete();
              });
          }
          CompositeFuture.all(updatedHoldingsRecordFutures)
            .onSuccess(ar -> {
              if (!expiredHoldings.isEmpty()) {
                processOLError(dataImportEventPayload, future, holdingsRecordCollection, expiredHoldings.get(0), errors);
              }
              String errorsAsStringJson = Json.encode(errors);
              if (!updatedHoldingsRecord.isEmpty() || errors.size() == 0) {
                LOGGER.warn(format("handle:: Errors during holdings update: %s", Json.encode(updatedHoldingsRecord)));
                dataImportEventPayload.getContext().put(ERRORS, errorsAsStringJson);
                dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encode(updatedHoldingsRecord));
                future.complete(dataImportEventPayload);
              } else {
                future.completeExceptionally(new EventProcessingException(errorsAsStringJson));
              }
            })
            .onFailure(future::completeExceptionally);
        })
        .onFailure(e -> {
          LOGGER.warn("handle:: Error updating inventory Holdings by jobExecutionId: '{}'", jobExecutionId, e);
          future.completeExceptionally(e);
        });
    } catch (Exception e) {
      LOGGER.warn("handle:: Failed to update Holdings", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private static void convertHoldings(DataImportEventPayload dataImportEventPayload) {
    JsonArray holdingsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    for (int i = 0; i < holdingsJsonArray.size(); i++) {
      JsonObject holdingAsJson = holdingsJsonArray.getJsonObject(i);
      if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
        holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
        holdingsJsonArray.set(i, holdingAsJson);
      }
    }
    dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingsJsonArray.encode());
  }

  private static void validateRequiredHoldingsFields(DataImportEventPayload dataImportEventPayload, List<PartialError> errors) {
    JsonArray holdingsList = new JsonArray(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    for (int i = 0; i < holdingsList.size(); i++) {

      JsonObject holdingAsJson = holdingsList.getJsonObject(i);
      if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
        holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
        HoldingsRecord tmpHoldingsRecord = Json.decodeValue(String.valueOf(holdingAsJson), HoldingsRecord.class);
        String holdingId = tmpHoldingsRecord.getId();
        String hrid = tmpHoldingsRecord.getHrid();
        String instanceId = tmpHoldingsRecord.getInstanceId();
        String permanentLocationId = tmpHoldingsRecord.getPermanentLocationId();
        if (StringUtils.isAnyBlank(hrid, instanceId, permanentLocationId, holdingId)) {
          LOGGER.warn("validateRequiredHoldingsFields:: Can`t update Holding entity hrid: {}, instanceId: {}, permanentLocationId: {}, holdingId: {}", hrid, instanceId, permanentLocationId, holdingId);
          errors.add(new PartialError(holdingId != null ? holdingId : BLANK, EMPTY_REQUIRED_FIELDS_ERROR_MESSAGE));
        }
      }
    }
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == HOLDINGS;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getContext().put(CURRENT_EVENT_TYPE_PROPERTY, dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(CURRENT_NODE_PROPERTY, Json.encode(dataImportEventPayload.getCurrentNode()));
    dataImportEventPayload.getContext().put(CURRENT_HOLDING_PROPERTY, Json.encode(dataImportEventPayload.getContext().get(HOLDINGS.value())));

    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());

    JsonArray holdingsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    for (int i = 0; i < holdingsJsonArray.size(); i++) {
      JsonObject holdingAsJson = holdingsJsonArray.getJsonObject(i);
      holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null ? holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) : holdingAsJson;
      holdingsJsonArray.set(i, new JsonObject().put(HOLDINGS_PATH_FIELD, holdingAsJson));
    }
    dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingsJsonArray.encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void processOLError(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, HoldingsRecordCollection holdingsRecords, HoldingsRecord holding, List<PartialError> errors) {
    int currentRetryNumber = dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER) == null ? 0 : Integer.parseInt(dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      dataImportEventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("processOLError:: Error updating Holding by id '{}'. Retry UpdateHoldingEventHandler handler...", holding.getId());
      holdingsRecords.findById(holding.getId())
        .thenAccept(actualHolding -> prepareDataAndReInvokeCurrentHandler(dataImportEventPayload, future, actualHolding))
        .thenAccept(v -> dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER))
        .exceptionally(e -> {
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Cannot get actual Holding by id: '%s' for jobExecutionId '%s'. Error: %s ", holding.getId(), dataImportEventPayload.getJobExecutionId(), e.getCause());
          LOGGER.warn("processOLError:: " + errMessage);
          errors.add(new PartialError(holding.getId() != null ? holding.getId() : BLANK, errMessage));
          future.complete(dataImportEventPayload);
          return null;
        });
    } else {
      dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Holding update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, dataImportEventPayload.getJobExecutionId());
      LOGGER.warn("processOLError:: " + errMessage);
      errors.add(new PartialError(holding.getId() != null ? holding.getId() : BLANK, errMessage));
      future.complete(dataImportEventPayload);
    }
  }

  private void prepareDataAndReInvokeCurrentHandler(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, HoldingsRecord actualHolding) {
    List<HoldingsRecord> holdingsList = List.of(Json.decodeValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
    List<HoldingsRecord> updatedHoldingsList = new ArrayList<>(holdingsList);
    for (int i = 0; i < holdingsList.size(); i++) {
      HoldingsRecord holdingsRecord = holdingsList.get(i);
      if (holdingsRecord.getId().equals(actualHolding.getId())) {
        updatedHoldingsList.set(i, actualHolding);
      }
    }

    JsonArray resultedHoldings = new JsonArray();
    for (HoldingsRecord currentHolding : updatedHoldingsList) {
      resultedHoldings.add(new JsonObject().put(HOLDINGS_PATH_FIELD, new JsonObject(Json.encode(currentHolding))));
    }
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encode(resultedHoldings)); // Convert to Array (find by id specific Holdings in array and update just current entity by id and reinvoke one more time)
    dataImportEventPayload.getEventsChain().remove(dataImportEventPayload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
    try {
      dataImportEventPayload.setCurrentNode(ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
    } catch (JsonProcessingException e) {
      LOGGER.warn(format("prepareDataAndReInvokeCurrentHandler:: Cannot map from CURRENT_NODE value %s", e.getCause()));
    }
    dataImportEventPayload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
    dataImportEventPayload.getContext().remove(CURRENT_NODE_PROPERTY);
    dataImportEventPayload.getContext().remove(CURRENT_HOLDING_PROPERTY);
    handle(dataImportEventPayload).whenComplete((res, e) -> future.complete(dataImportEventPayload));
  }

  private void constructDataImportEventPayload(Promise<Void> promise, DataImportEventPayload dataImportEventPayload, List<HoldingsRecord> holdings, Context context, List<PartialError> errors) {
    if (!isPayloadConstructed) {
      isPayloadConstructed = true;
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      payloadContext.put(HOLDINGS.value(), Json.encodePrettily(holdings));
      if (payloadContext.containsKey(ITEM.value())) {
        ItemCollection itemCollection = storage.getItemCollection(context);
        updateDataImportEventPayloadItem(promise, dataImportEventPayload, itemCollection, errors);
      } else {
        promise.complete();
      }
    } else {
      promise.complete();
    }
  }

  private void updateDataImportEventPayloadItem(Promise<Void> future, DataImportEventPayload dataImportEventPayload, ItemCollection itemCollection, List<PartialError> errors) {
    JsonArray oldItemsAsJson = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    JsonArray resultedItemsList = new JsonArray();

    for (int i = 0; i < oldItemsAsJson.size(); i++) {
      JsonObject singleItemAsJson = oldItemsAsJson.getJsonObject(i);
      String itemId = singleItemAsJson.getJsonObject("item").getString(ITEM_ID_HEADER);
      itemCollection.findById(itemId, findResult -> {
        if (Objects.nonNull(findResult)) {
          JsonObject itemAsJson = new JsonObject(ItemUtil.mapToMappingResultRepresentation(findResult.getResult()));
          resultedItemsList.add(itemAsJson);
          dataImportEventPayload.getContext().put(ITEM.value(), Json.encode(itemAsJson));
        }
        future.complete();
      }, failure -> {
        errors.add(new PartialError(itemId != null ? itemId : BLANK, failure.getReason()));
        EventProcessingException processingException =
          new EventProcessingException(format(CANNOT_GET_ACTUAL_ITEM_MESSAGE, itemId, failure.getReason(), failure.getStatusCode()));
        LOGGER.warn("updateDataImportEventPayloadItem:: " + processingException);
        future.complete();
      });
    }
    dataImportEventPayload.getContext().put(ITEM.value(), resultedItemsList.encode());
  }
}
