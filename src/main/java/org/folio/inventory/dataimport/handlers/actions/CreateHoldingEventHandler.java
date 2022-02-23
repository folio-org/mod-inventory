package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.logging.log4j.util.Strings.isNotEmpty;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateHoldingEventHandler.class);
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String HOLDINGS_PATH_FIELD = "holdings";
  private static final String PERMANENT_LOCATION_ID_FIELD = "permanentLocationId";
  private static final String PERMANENT_LOCATION_ID_ERROR_MESSAGE = "Can`t create Holding entity: 'permanentLocationId' is empty";
  private static final String CREATE_HOLDING_ERROR_MESSAGE = "Failed to create Holdings";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t create Holding entity: context is empty or doesn`t exists";
  private static final String PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG = "Failed to extract instanceId from instance entity or parsed record";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create a Holding entity requires a mapping profile";
  private static final String FOLIO_SOURCE_ID = "f32d531e-df79-46b3-8932-cdd35f7a2264";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;
  private final IdStorageService idStorageService;

  public CreateHoldingEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
    this.idStorageService = idStorageService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
        || StringUtils.isEmpty(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))) {
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      String recordId = payloadContext.get(RECORD_ID_HEADER);
      String chunkId = payloadContext.get(CHUNK_ID_HEADER);

      Future<RecordToEntity> recordToHoldingsFuture = idStorageService.store(recordId, UUID.randomUUID().toString(), dataImportEventPayload.getTenant());
      recordToHoldingsFuture.onSuccess(res -> {
          String holdingsId = res.getEntityId();
          mappingMetadataCache.get(jobExecutionId, context)
            .map(parametersOptional -> parametersOptional.orElseThrow(() ->
              new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG,
                jobExecutionId, recordId, chunkId))))
            .map(mappingMetadataDto -> {
              prepareEvent(dataImportEventPayload);
              MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
              MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

              JsonObject holdingAsJson = new JsonObject(payloadContext.get(HOLDINGS.value()));
              if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
                holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
              }
              holdingAsJson.put("id", holdingsId);
              holdingAsJson.put("sourceId", FOLIO_SOURCE_ID);
              fillInstanceIdIfNeeded(dataImportEventPayload, holdingAsJson);
              checkIfPermanentLocationIdExists(holdingAsJson);
              return Json.decodeValue(payloadContext.get(HOLDINGS.value()), HoldingsRecord.class);
            })
            .compose(holdingToCreate -> addHoldings(holdingToCreate, context))
            .onSuccess(createdHoldings -> {
              LOGGER.info("Created Holding record by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}'",
                jobExecutionId, recordId, chunkId);
              payloadContext.put(HOLDINGS.value(), Json.encodePrettily(createdHoldings));
              future.complete(dataImportEventPayload);
            })
            .onFailure(e -> {
              if (!(e instanceof DuplicateEventException)) {
                LOGGER.error("Error creating inventory Holding record by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
                  recordId, chunkId, e);
              }
              future.completeExceptionally(e);
            });
        })
        .onFailure(failure -> {
          LOGGER.error("Error creating inventory recordId and holdingsId relationship by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ",
            jobExecutionId, recordId, chunkId, failure);
          future.completeExceptionally(failure);
        });
    } catch (Exception e) {
      LOGGER.error(CREATE_HOLDING_ERROR_MESSAGE, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()) != null && dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == ActionProfile.Action.CREATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.HOLDINGS;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void fillInstanceIdIfNeeded(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson) {
    if (isBlank(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      String instanceId = null;
      String instanceAsString = dataImportEventPayload.getContext().get(EntityType.INSTANCE.value());

      if (isNotEmpty(instanceAsString)) {
        JsonObject holdingsRecord = new JsonObject(instanceAsString);
        instanceId = holdingsRecord.getString("id");
      }
      if (isBlank(instanceId)) {
        String recordAsString = dataImportEventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value());
        Record record = Json.decodeValue(recordAsString, Record.class);
        instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      }
      if (isBlank(instanceId)) {
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG);
      }
      fillInstanceId(dataImportEventPayload, holdingAsJson, instanceId);
    }
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

  private Future<HoldingsRecord> addHoldings(HoldingsRecord holdings, Context context) {
    Promise<HoldingsRecord> promise = Promise.promise();
    HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
    holdingsRecordCollection.add(holdings,
      success -> promise.complete(success.getResult()),
      failure -> {
        //for now there is a solution via error-message contains. It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOGGER.info("Duplicated event received by InstanceId: {}. Ignoring...", holdings.getId());
          promise.fail(new DuplicateEventException("Duplicated event"));
        } else {
          LOGGER.error(format("Error posting Holdings cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }
}
