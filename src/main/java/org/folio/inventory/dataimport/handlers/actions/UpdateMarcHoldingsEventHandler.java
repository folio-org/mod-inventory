package org.folio.inventory.dataimport.handlers.actions;

import static io.vertx.core.json.JsonObject.mapFrom;
import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isBlank;

import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.LoggerUtil.INCOMING_RECORD_ID;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.dataimport.util.ParsedRecordUtil.getControlFieldValue;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

import java.io.UnsupportedEncodingException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.DataImportEventPayload;
import org.folio.Holdings;
import org.folio.HoldingsRecord;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;

public class UpdateMarcHoldingsEventHandler implements EventHandler {

  protected static final Logger LOGGER = LogManager.getLogger(UpdateMarcHoldingsEventHandler.class);

  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String FIELD_004_MARC_HOLDINGS_NOT_NULL = "The field 004 for marc holdings must be not null";
  private static final String INSTANCE_HRID_TAG = "004";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_HOLDING_PROPERTY = "CURRENT_HOLDING";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found.";
  private static final String META_INFO_MSG_PATTERN = "JobExecutionId: '%s', RecordId: '%s', ChunkId: '%s'";
  private static final String ACTION_SUCCEED_MSG_PATTERN = "Action '%s' for record '%s' succeed.";
  private static final String ACTION_FAILED_MSG_PATTERN = "Action '%s' for record '%s' failed.";
  private static final String UNEXPECTED_PAYLOAD_MSG = "Unexpected payload";
  private static final String CANNOT_UPDATE_HOLDING_ERROR_MESSAGE = "Error updating Holding by holdingId %s and jobExecution '%s', failure reason: %s, status code %s";
  private static final String ERROR_HOLDING_MSG = "Error loading inventory holdings for MARC BIB";
  private static final String HOLDING_NOT_FOUND_ERROR_MESSAGE = "Holdings record was not found by id: %s";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;
  private final KafkaEventPublisher eventPublisher;

  public UpdateMarcHoldingsEventHandler(Storage storage,
    MappingMetadataCache mappingMetadataCache,
    KafkaEventPublisher eventPublisher) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
    this.eventPublisher = eventPublisher;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    logParametersEventHandler(LOGGER, payload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (!isExpectedPayload(payload)) {
        LOGGER.warn("Payload is not expected");
        throw new EventProcessingException(UNEXPECTED_PAYLOAD_MSG);
      }

      prepareEvent(payload);

      var context = constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl(), payload.getContext().get(EventHandlingUtil.USER_ID));
      var jobExecutionId = payload.getJobExecutionId();
      LOGGER.info("Update marc holding with jobExecutionId: {}, incomingRecordId: {}",
        jobExecutionId, payload.getContext().get(INCOMING_RECORD_ID));

      mappingMetadataCache.get(jobExecutionId, context)
        .map(mapMetadataOrFail())
        .compose(mappingMetadata -> mapHolding(payload, mappingMetadata))
        .compose(holdings -> fillInstanceIdByHrid(payload, holdings, context))
        .compose(holdings -> processHolding(holdings, context, payload))
        .onSuccess(successHandler(payload, future))
        .onFailure(failureHandler(payload, future));
    } catch (Exception e) {
      LOGGER.error("Failed to update Holding", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (payload.getCurrentNode() != null && getMarcHoldingRecordAsString(payload) != null
      && MAPPING_PROFILE == payload.getCurrentNode().getContentType()) {
      var mappingProfile = mapFrom(payload.getCurrentNode().getContent()).mapTo(MappingProfile.class);
      return mappingProfile.getExistingRecordType() == EntityType.fromValue(MARC_HOLDINGS.value());
    }
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_HOLDINGS_UPDATED_READY_FOR_POST_PROCESSING.value();
  }

  private boolean isExpectedPayload(DataImportEventPayload payload) {
    return payload != null
      && payload.getCurrentNode() != null
      && MAPPING_PROFILE == payload.getCurrentNode().getContentType()
      && payload.getContext() != null
      && !payload.getContext().isEmpty()
      && StringUtils.isNotBlank(getMarcHoldingRecordAsString(payload));
  }

  private Function<Optional<MappingMetadataDto>, MappingMetadataDto> mapMetadataOrFail() {
    return parameters -> parameters.orElseThrow(() -> new EventProcessingException(MAPPING_METADATA_NOT_FOUND_MSG));
  }

  private void prepareEvent(DataImportEventPayload payload) {
    payload.setEventType(DI_INVENTORY_HOLDING_UPDATED.value());
    payload.getEventsChain().add(payload.getEventType());
    payload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
    payload.getContext().put(CURRENT_EVENT_TYPE_PROPERTY, payload.getEventType());
    payload.getContext().put(CURRENT_NODE_PROPERTY, Json.encode(payload.getCurrentNode()));
    payload.getContext().put(CURRENT_HOLDING_PROPERTY, Json.encode(payload.getContext().get(HOLDINGS.value())));
  }

  private Future<Holdings> mapHolding(DataImportEventPayload payload, MappingMetadataDto mappingMetadata) {
    try {
      var mappingRules = new JsonObject(mappingMetadata.getMappingRules());
      var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
      var marcRecord = new JsonObject(getMarcHoldingRecordAsString(payload)).mapTo(Record.class);
      var parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String holdingsId = marcRecord.getExternalIdsHolder().getHoldingsId();
      LOGGER.info("Holdings update with holdingId: {}", holdingsId);
      RecordMapper<Holdings> recordMapper = RecordMapperBuilder.buildMapper(MARC_HOLDINGS.value());
      var holdings = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      holdings.setId(holdingsId);
      return Future.succeededFuture(holdings);
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Holdings", e);
      return Future.failedFuture(new JsonMappingException("Error in default mapper.", e));
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private Future<HoldingsRecord> processHolding(Holdings holdings, Context context, DataImportEventPayload payload) {
    var holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
    HoldingsRecord mappedRecord = Json.decodeValue(Json.encode(JsonObject.mapFrom(holdings)), HoldingsRecord.class);
    Promise<HoldingsRecord> promise = Promise.promise();
    holdingsRecordCollection.findById(mappedRecord.getId()).thenAccept(actualRecord -> {
      if (actualRecord == null) {
        promise.fail(new EventProcessingException(format(HOLDING_NOT_FOUND_ERROR_MESSAGE, mappedRecord.getId())));
        return;
      }

      mappedRecord.setVersion(actualRecord.getVersion());
      holdingsRecordCollection.update(mappedRecord,
        success -> {
          holdings.setVersion(mappedRecord.getVersion());
          holdings.setInstanceId(mappedRecord.getInstanceId());
          payload.getContext().put(HOLDINGS.value(), Json.encode(holdings));
          promise.complete(mappedRecord);
        },
        failure -> failureUpdateHandler(payload, mappedRecord.getId(), holdingsRecordCollection, promise, failure));
    });
    return promise.future();
  }

  private Future<Holdings> fillInstanceIdByHrid(DataImportEventPayload dataImportEventPayload, Holdings holdings, Context context) {
    Promise<Holdings> promise = Promise.promise();
    if (StringUtils.isBlank(holdings.getInstanceId())) {
      var rec = Json.decodeValue(getMarcHoldingRecordAsString(dataImportEventPayload), Record.class);
      var instanceHrid = getControlFieldValue(rec, INSTANCE_HRID_TAG);
      if (isBlank(instanceHrid)) {
        LOGGER.warn("FIELD_004_MARC_HOLDINGS_NOT_NULL");
        promise.fail(new EventProcessingException(FIELD_004_MARC_HOLDINGS_NOT_NULL));
      } else {
        var instanceCollection = storage.getInstanceCollection(context);
        fillInstanceIdSearchingByHrid(instanceCollection, instanceHrid, holdings, promise);
      }
    } else {
      promise.complete(holdings);
    }
    return promise.future();
  }

  private void fillInstanceIdSearchingByHrid(InstanceCollection instanceCollection,
                                             String instanceHrid, Holdings holdings,
                                             Promise<Holdings> promise) {
    try {
      instanceCollection.findByCql(format("hrid==%s", instanceHrid), PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
            var instanceIdFromDb = findResult.getResult().records.get(0).getId();
            holdings.setInstanceId(instanceIdFromDb);
            promise.complete(holdings);
          } else {
            promise.fail(new EventProcessingException("No instance id found for marc holdings with hrid: " + instanceHrid));
          }
        },
        failure -> {
          LOGGER.error(format(ERROR_HOLDING_MSG + ". StatusCode: %s. Message: %s", failure.getStatusCode(), failure.getReason()));
          promise.fail(new EventProcessingException(failure.getReason()));
        });
    } catch (UnsupportedEncodingException e) {
      LOGGER.error(ERROR_HOLDING_MSG, e);
      promise.fail(e);
    }
  }

  private void failureUpdateHandler(DataImportEventPayload payload, String id, HoldingsRecordCollection collection, Promise<HoldingsRecord> promise, Failure failure) {
    if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
      processOLError(failure, payload, id, collection, promise);
    } else {
      promise.fail(new DataImportException(format(CANNOT_UPDATE_HOLDING_ERROR_MESSAGE,
        id, payload.getJobExecutionId(), failure.getReason(), failure.getStatusCode())));
    }
  }

  private Handler<Throwable> failureHandler(DataImportEventPayload payload,
                                            CompletableFuture<DataImportEventPayload> future) {
    return e -> {
      LOGGER.error(() -> constructMsg(format(ACTION_FAILED_MSG_PATTERN, UPDATE, HOLDINGS), payload), e);
      future.completeExceptionally(e);
    };
  }

  private Handler<HoldingsRecord> successHandler(DataImportEventPayload payload,
                                                 CompletableFuture<DataImportEventPayload> future) {
    return holdings -> {
      LOGGER.info(() -> constructMsg(format(ACTION_SUCCEED_MSG_PATTERN, UPDATE, HOLDINGS), payload));
      DataImportEventPayload copiedPayload = copyEventPayloadWithoutCurrentNode(payload);
      eventPublisher.publish(copiedPayload);
      future.complete(copiedPayload);
    };
  }

  private String getMarcHoldingRecordAsString(DataImportEventPayload dataImportEventPayload) {
    return dataImportEventPayload.getContext().get(MARC_HOLDINGS.value());
  }

  /**
   * This method handles the Optimistic Locking error.
   * The Optimistic Locking error occurs when the updating record has matched and has not updated yet.
   * In this time some another request wants to update the matched record. This happens rarely, however it has a place to be.
   * In such case the method retries a record update:
   * - it calls this 'UpdateMarcHoldingsEventHandler' again keeping a number of calls(retries) in the event context;
   * - when a number of retries exceeded the maximum limit (see <>MAX_RETRIES_COUNT</>) then event handling goes ahead as usual.
   */
  private void processOLError(Failure failure, DataImportEventPayload payload, String recordId, HoldingsRecordCollection recordCollection, Promise<HoldingsRecord> promise) {
    int currentRetryNumber = payload.getContext().get(CURRENT_RETRY_NUMBER) == null ? 0 : Integer.parseInt(payload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      payload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Holding by id '{}' - '{}', status code '{}'. Retry UpdateMarcHoldingsEventHandler handler...", recordId, failure.getReason(), failure.getStatusCode());
      recordCollection.findById(recordId)
        .thenAccept(actualRecord -> prepareDataAndReInvokeCurrentHandler(payload, promise, actualRecord))
        .exceptionally(e -> {
          payload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Cannot get actual Holding by id: '%s' for jobExecutionId '%s'. Error: %s ", recordId, payload.getJobExecutionId(), e.getCause());
          LOGGER.error(errMessage);
          promise.fail(new EventProcessingException(errMessage));
          return null;
        });
    } else {
      payload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Holding update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, payload.getJobExecutionId());
      LOGGER.error(errMessage);
      promise.fail(new EventProcessingException(errMessage));
    }
  }

  private void prepareDataAndReInvokeCurrentHandler(DataImportEventPayload payload, Promise<HoldingsRecord> promise, HoldingsRecord actualHoldings) {
    payload.getContext().put(HOLDINGS.value(), Json.encode(JsonObject.mapFrom(actualHoldings)));
    payload.getEventsChain().remove(payload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
    try {
      payload.setCurrentNode(ObjectMapperTool.getMapper().readValue(payload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
    } catch (JsonProcessingException e) {
      LOGGER.error(format("Cannot map from CURRENT_NODE value %s", e.getCause()));
    }
    payload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
    payload.getContext().remove(CURRENT_NODE_PROPERTY);
    payload.getContext().remove(CURRENT_HOLDING_PROPERTY);
    handle(payload).whenComplete((res, e) -> {
      if (e != null) {
        promise.fail(new EventProcessingException(e.getMessage()));
      } else {
        promise.complete(actualHoldings);
      }
    });
  }

  protected String constructMsg(String message, DataImportEventPayload payload) {
    if (payload == null) {
      return message;
    } else {
      return message + " " + constructMetaInfoMsg(payload);
    }
  }

  private String constructMetaInfoMsg(DataImportEventPayload payload) {
    return format(
      META_INFO_MSG_PATTERN,
      payload.getJobExecutionId(),
      getRecordIdHeader(payload),
      getChunkIdHeader(payload)
    );
  }

  private String getChunkIdHeader(DataImportEventPayload payload) {
    return payload.getContext() == null ? "-" : payload.getContext().get(CHUNK_ID_HEADER);
  }

  private String getRecordIdHeader(DataImportEventPayload payload) {
    return payload.getContext() == null ? "-" : payload.getContext().get(RECORD_ID_HEADER);
  }

  private DataImportEventPayload copyEventPayloadWithoutCurrentNode(DataImportEventPayload payload) {
    DataImportEventPayload newPayload = new DataImportEventPayload();
    newPayload.setEventType(payload.getEventType());
    newPayload.setEventsChain(payload.getEventsChain());
    newPayload.setContext(payload.getContext());
    newPayload.setToken(payload.getToken());
    newPayload.setJobExecutionId(payload.getJobExecutionId());
    newPayload.setOkapiUrl(payload.getOkapiUrl());
    newPayload.setProfileSnapshot(payload.getProfileSnapshot());
    newPayload.setTenant(payload.getTenant());
    return newPayload;
  }

}
