package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.ParsedRecordUtil.getControlFieldValue;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.Holdings;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

public class CreateMarcHoldingsEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateMarcHoldingsEventHandler.class);
  private static final String ERROR_HOLDING_MSG = "Error loading inventory holdings for MARC BIB";
  private static final String MARC_FORMAT = "MARC_HOLDINGS";
  private static final String HOLDINGS_PATH = "holdings";
  private static final String HOLDINGS_PATH_FIELD = "holdings";
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String PERMANENT_LOCATION_ID_FIELD = "permanentLocationId";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'";
  private static final String PERMANENT_LOCATION_ID_ERROR_MESSAGE = "Can`t create Holding entity: 'permanentLocationId' is empty";
  private static final String SAVE_HOLDING_ERROR_MESSAGE = "Can`t save new holding";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t create Holding entity: context is empty or doesn`t exists";
  private static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create a Holding entity requires a mapping profile";
  private static final String FIELD_004_MARC_HOLDINGS_NOT_NULL = "The field 004 for marc holdings must be not null";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  public CreateMarcHoldingsEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
        dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
        || StringUtils.isEmpty(dataImportEventPayload.getContext().get(MARC_HOLDINGS.value()))) {
        return CompletableFuture.failedFuture(new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      var context = constructContext(dataImportEventPayload.getTenant(),
        dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Record record = new JsonObject(payloadContext.get(EntityType.MARC_HOLDINGS.value()))
        .mapTo(Record.class);
      prepareEvent(dataImportEventPayload);

      mappingMetadataCache.get(dataImportEventPayload.getJobExecutionId(), context)
        .map(parametersOptional -> parametersOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, dataImportEventPayload.getJobExecutionId()))))
        .onSuccess(mappingMetadata -> defaultMapRecordToHoldings(dataImportEventPayload, mappingMetadata))
        .map(v -> processMappingResult(dataImportEventPayload, record))
        .compose(holdingJson -> findInstanceIdByHrid(dataImportEventPayload, holdingJson, context)
          .compose(instanceId -> {
            fillInstanceId(dataImportEventPayload, holdingJson, instanceId);
            var holdingsRecords = storage.getHoldingsRecordCollection(context);
            HoldingsRecord holding = Json.decodeValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord.class);
            return addHoldings(holding, holdingsRecords);
          }))
        .onSuccess(createdHoldings -> {
          LOGGER.info("Created Holding record");
          dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(createdHoldings));
          future.complete(dataImportEventPayload);
        })
        .onFailure(e -> {
          LOGGER.error(SAVE_HOLDING_ERROR_MESSAGE, e);
          future.completeExceptionally(e);
        });
    } catch (Exception e) {
      LOGGER.error("Failed to create Holdings", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private JsonObject processMappingResult(DataImportEventPayload dataImportEventPayload, Record record) {
    var holdingAsJson = new JsonObject(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
      holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
    }
    holdingAsJson.put("id", record.getId());
    holdingAsJson.remove("hrid");
    checkIfPermanentLocationIdExists(holdingAsJson);

    LOGGER.debug("Creating holdings with id: {}", record.getId());

    return holdingAsJson;
  }

  private void defaultMapRecordToHoldings(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadata) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      var mappingRules = new JsonObject(mappingMetadata.getMappingRules());
      var parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_HOLDINGS.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
      RecordMapper<Holdings> recordMapper = RecordMapperBuilder.buildMapper(MARC_FORMAT);
      var holdings = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encode(new JsonObject().put(HOLDINGS_PATH, JsonObject.mapFrom(holdings))));
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Holdings", e);
      throw new JsonMappingException("Error in default mapper.", e);
    }
  }

  private Future<String> findInstanceIdByHrid(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson, Context context) {
    Promise<String> promise = Promise.promise();
    if (StringUtils.isBlank(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      var recordAsString = dataImportEventPayload.getContext().get("MARC_HOLDINGS");
      var record = Json.decodeValue(recordAsString, Record.class);
      var instanceHrid = getControlFieldValue(record, "004");
      if (isBlank(instanceHrid)) {
        throw new EventProcessingException(FIELD_004_MARC_HOLDINGS_NOT_NULL);
      }
      var instanceCollection = storage.getInstanceCollection(context);
      try {
        instanceCollection.findByCql(format("hrid=%s", instanceHrid), PagingParameters.defaults(),
          findResult -> {
            String instanceId = null;
            if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
              var records = findResult.getResult().records;
              var instance = records.stream()
                .findFirst()
                .orElseThrow(() -> new EventProcessingException("No instance id found for marc holdings with hrid: " + instanceHrid));
              instanceId = instance.getId();
            }
            promise.complete(instanceId);
          },
          failure -> {
            LOGGER.error(ERROR_HOLDING_MSG + format(". StatusCode: %s. Message: %s", failure.getStatusCode(), failure.getReason()));
            promise.fail(new EventProcessingException(failure.getReason()));
          });
      } catch (UnsupportedEncodingException e) {
        LOGGER.error(ERROR_HOLDING_MSG, e);
        promise.fail(e);
      }
    }
    return promise.future();
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && dataImportEventPayload.getContext().get(MARC_HOLDINGS.value()) != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      var actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == ActionProfile.Action.CREATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.HOLDINGS;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value();
  }

  private void checkIfPermanentLocationIdExists(JsonObject holdingAsJson) {
    if (isEmpty(holdingAsJson.getString(PERMANENT_LOCATION_ID_FIELD))) {
      throw new EventProcessingException(PERMANENT_LOCATION_ID_ERROR_MESSAGE);
    }
  }

  private void fillInstanceId(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson, String instanceId) {
    holdingAsJson.put(INSTANCE_ID_FIELD, instanceId);
    dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingAsJson.encode());
  }

  private Future<HoldingsRecord> addHoldings(HoldingsRecord holdings, HoldingsRecordCollection holdingsRecordCollection) {
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.add(holdings,
      success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error posting Holdings cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
  }

}
