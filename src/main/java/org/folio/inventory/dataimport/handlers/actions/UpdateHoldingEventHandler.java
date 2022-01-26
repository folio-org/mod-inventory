package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
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
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
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
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_HOLDING_PROPERTY = "CURRENT_HOLDING";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String CANNOT_UPDATE_HOLDING_ERROR_MESSAGE = "Error updating Holding by holdingId '%s' and jobExecution '%s' recordId '%s' chunkId '%s' - %s, status code %s";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  public UpdateHoldingEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_UPDATED.value());

      if (dataImportEventPayload.getContext() == null
        || isEmpty(dataImportEventPayload.getContext().get(HOLDINGS.value()))
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))) {
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      HoldingsRecord tmpHoldingsRecord = retrieveHolding(dataImportEventPayload.getContext());

      String holdingId = tmpHoldingsRecord.getId();
      String hrid = tmpHoldingsRecord.getHrid();
      String instanceId = tmpHoldingsRecord.getInstanceId();
      String permanentLocationId = tmpHoldingsRecord.getPermanentLocationId();
      if (StringUtils.isAnyBlank(hrid, instanceId, permanentLocationId, holdingId)) {
        throw new EventProcessingException(EMPTY_REQUIRED_FIELDS_ERROR_MESSAGE);
      }

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();

      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      mappingMetadataCache.get(jobExecutionId, context)
        .map(parametersOptional -> parametersOptional
          .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MESSAGE, jobExecutionId,
            recordId, chunkId))))
        .onSuccess(mappingMetadataDto -> {
          prepareEvent(dataImportEventPayload);
          MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
          MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

          HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
          HoldingsRecord holding = retrieveHolding(dataImportEventPayload.getContext());

          holdingsRecords.update(holding, holdingSuccess -> constructDataImportEventPayload(future, dataImportEventPayload, holding),
            failure -> {
              if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
                processOLError(dataImportEventPayload, future, holdingsRecords, holding, failure);
              } else {
                dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
                LOGGER.error(format(CANNOT_UPDATE_HOLDING_ERROR_MESSAGE, holding.getId(), jobExecutionId, recordId, chunkId, failure.getReason(), failure.getStatusCode()));
                future.completeExceptionally(new EventProcessingException(format(UPDATE_HOLDING_ERROR_MESSAGE, jobExecutionId,
                  recordId, chunkId)));
              }
            });
        })
        .onFailure(e -> {
          LOGGER.error("Error updating inventory Holdings by jobExecutionId: '{}'", jobExecutionId, e);
          future.completeExceptionally(e);
        });
    } catch (Exception e) {
      LOGGER.error("Failed to update Holdings", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == HOLDINGS;
    }
    return false;
  }

  private HoldingsRecord retrieveHolding(HashMap<String, String> context) {
    return (isNull(new JsonObject(context.get(HOLDINGS.value())).getJsonObject(HOLDINGS_PATH_FIELD))) ?
      Json.decodeValue(context.get(HOLDINGS.value()), HoldingsRecord.class) :
      Json.decodeValue(String.valueOf(new JsonObject(context.get(HOLDINGS.value())).getJsonObject(HOLDINGS_PATH_FIELD)), HoldingsRecord.class);
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getContext().put(CURRENT_EVENT_TYPE_PROPERTY, dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(CURRENT_NODE_PROPERTY, Json.encode(dataImportEventPayload.getCurrentNode()));
    dataImportEventPayload.getContext().put(CURRENT_HOLDING_PROPERTY, Json.encode(dataImportEventPayload.getContext().get(HOLDINGS.value())));

    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    JsonObject jsonHoldings = new JsonObject(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().put(HOLDINGS_PATH_FIELD, jsonHoldings).encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void processOLError(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, HoldingsRecordCollection holdingsRecords, HoldingsRecord holding, Failure failure) {
    int currentRetryNumber = dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER)  == null ? 0 : Integer.parseInt(dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      dataImportEventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Holding by id '{}' - '{}', status code '{}'. Retry UpdateHoldingEventHandler handler...", holding.getId(), failure.getReason(), failure.getStatusCode());
      holdingsRecords.findById(holding.getId())
        .thenAccept(actualInstance -> prepareDataAndReInvokeCurrentHandler(dataImportEventPayload, future, actualInstance))
        .exceptionally(e -> {
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.error(format("Cannot get actual Holding by id: '%s'. Error: %s ", holding.getId(), e.getCause()));
          future.completeExceptionally(new EventProcessingException(format("Cannot get actual Holding by id: '%s'. Error: '%s'", holding.getId(), e.getCause())));
          return null;
        });
    } else {
      dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      LOGGER.error("Current retry number {} exceeded or equal given number {} for the Holding update", MAX_RETRIES_COUNT, currentRetryNumber);
      future.completeExceptionally(new EventProcessingException(format("Current retry number %s exceeded or equal given number %s for the Holding update", MAX_RETRIES_COUNT, currentRetryNumber)));
    }
  }

  private void prepareDataAndReInvokeCurrentHandler(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, HoldingsRecord actualHolding) {
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encode(JsonObject.mapFrom(actualHolding)));
    dataImportEventPayload.getEventsChain().remove(dataImportEventPayload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
    try {
      dataImportEventPayload.setCurrentNode(ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
    } catch (JsonProcessingException e) {
      LOGGER.error(format("Cannot map from CURRENT_NODE value %s", e.getCause()));
    }
    dataImportEventPayload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
    dataImportEventPayload.getContext().remove(CURRENT_NODE_PROPERTY);
    dataImportEventPayload.getContext().remove(CURRENT_HOLDING_PROPERTY);
    handle(dataImportEventPayload).whenComplete((res, e) -> {
      if (e != null) {
        future.completeExceptionally(new EventProcessingException(e.getMessage()));
      } else {
        future.complete(dataImportEventPayload);
      }
    });
  }

  private void constructDataImportEventPayload(CompletableFuture<DataImportEventPayload> future, DataImportEventPayload dataImportEventPayload, HoldingsRecord holding) {
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(holding));
    future.complete(dataImportEventPayload);
    future.complete(dataImportEventPayload);
  }
}
