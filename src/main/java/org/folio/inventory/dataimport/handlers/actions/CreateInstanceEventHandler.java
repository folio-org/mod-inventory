package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HttpStatus;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateInstanceEventHandler extends AbstractInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload - event payload context does not contain MARC_BIBLIOGRAPHIC data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create an Instance requires a mapping profile by jobExecutionId: '%s' and recordId: '%s'";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId: '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";

  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private MappingMetadataCache mappingMetadataCache;
  private IdStorageService idStorageService;
  private OrderHelperService orderHelperService;
  private HttpClient httpClient;

  public CreateInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                    MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService,
                                    OrderHelperService orderHelperService, HttpClient httpClient) {
    super(storage);
    this.orderHelperService = orderHelperService;
    this.mappingMetadataCache = mappingMetadataCache;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.idStorageService = idStorageService;
    this.httpClient = httpClient;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(format(ACTION_HAS_NO_MAPPING_MSG, jobExecutionId, recordId)));
      }

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Record targetRecord = Json.decodeValue(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()), Record.class);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      LOGGER.info("Create instance with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

      Future<RecordToEntity> recordToInstanceFuture = idStorageService.store(targetRecord.getId(), getInstanceId(targetRecord), dataImportEventPayload.getTenant());
      recordToInstanceFuture.onSuccess(res -> {
          String instanceId = res.getEntityId();
          mappingMetadataCache.get(jobExecutionId, context)
            .compose(parametersOptional -> parametersOptional
              .map(mappingMetadata -> {
                MappingParameters mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
                AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
                AdditionalFieldsUtil.move001To035(targetRecord);
                payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(targetRecord));
                return prepareAndExecuteMapping(dataImportEventPayload, new JsonObject(mappingMetadata.getMappingRules()), mappingParameters);
              })
              .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId, recordId, chunkId))))
            .compose(v -> {
              InstanceCollection instanceCollection = storage.getInstanceCollection(context);
              JsonObject instanceAsJson = prepareInstance(dataImportEventPayload, instanceId, jobExecutionId);
              List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
              if (!errors.isEmpty()) {
                String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", errors,
                  jobExecutionId, recordId, chunkId);
                LOGGER.warn(msg);
                return Future.failedFuture(msg);
              }

              Instance mappedInstance = Instance.fromJson(instanceAsJson);
              return addInstance(mappedInstance, instanceCollection)
                .compose(createdInstance -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(mappedInstance, context).map(createdInstance))
                .compose(createdInstance -> fill001And999IFieldsAndSetMatchedId(createdInstance, targetRecord))
                .compose(createdInstance -> saveRecordInSrsAndHandleResponse(dataImportEventPayload, targetRecord, createdInstance, instanceCollection));
            })
            .onSuccess(ar -> {
              dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(ar));
              orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload, DI_INVENTORY_INSTANCE_CREATED, context)
                .onComplete(result -> future.complete(dataImportEventPayload));
            })
            .onFailure(e -> {
              if (!(e instanceof DuplicateEventException)) {
                LOGGER.error("Error creating inventory Instance by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
                  recordId, chunkId, e);
              }
              future.completeExceptionally(e);
            });
        })
        .onFailure(failure -> {
          LOGGER.error("Error creating inventory recordId and instanceId relationship by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId, recordId,
            chunkId, failure);
          future.completeExceptionally(failure);
        });
    } catch (Exception e) {
      LOGGER.error("Error creating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Instance> fill001And999IFieldsAndSetMatchedId(Instance instance, Record record) {
    AdditionalFieldsUtil.fill001FieldInMarcRecord(record, instance.getHrid());
    if (StringUtils.isBlank(record.getMatchedId())) {
      record.setMatchedId(record.getId());
    }
    return AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', instance.getId())
      ? Future.succeededFuture(instance)
      : Future.failedFuture(format("Failed to add instance id '%s' to record with id '%s'", instance.getId(), record.getId()));
  }

  private Future<Instance> saveRecordInSrsAndHandleResponse(DataImportEventPayload payload, Record record,
                                                            Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(payload).postSourceStorageRecords(record)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(),
            Json.encode(encodeParsedRecordContent(result.bodyAsJson(Record.class))));
          LOGGER.info("Created MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}, jobExecutionId: {}",
            record.getId(), instance.getId(), payload.getTenant(), payload.getJobExecutionId());
          promise.complete(instance);
        } else {
          String msg = format("Failed to create MARC record in SRS, instanceId: '%s', jobExecutionId: '%s', status code: %s, Record: %s",
            instance.getId(), payload.getJobExecutionId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          deleteInstance(instance.getId(), payload.getJobExecutionId(), instanceCollection);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  private Record encodeParsedRecordContent(Record record) {
    ParsedRecord parsedRecord = record.getParsedRecord();
    if (parsedRecord != null) {
      parsedRecord.setContent(Json.encode(parsedRecord.getContent()));
      return record.withParsedRecord(parsedRecord);
    }
    return record;
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(DataImportEventPayload payload) {
    return new SourceStorageRecordsClient(payload.getOkapiUrl(), payload.getTenant(),
      payload.getToken(), httpClient);
  }

  private String getInstanceId(Record record) {
    String subfield999ffi = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    return isEmpty(subfield999ffi) ? UUID.randomUUID().toString() : subfield999ffi;
  }

  private JsonObject prepareInstance(DataImportEventPayload dataImportEventPayload, String instanceId, String jobExecutionId) {
    JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
    if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
      instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
    }
    instanceAsJson.put("id", instanceId);
    instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
    instanceAsJson.remove(HRID_KEY);

    LOGGER.debug("Creating instance with id: {} by jobExecutionId: {}", instanceId, jobExecutionId);
    return instanceAsJson;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value();
  }

  private Future<Void> prepareAndExecuteMapping(DataImportEventPayload dataImportEventPayload, JsonObject mappingRules, MappingParameters mappingParameters) {
    try {
      prepareEvent(dataImportEventPayload);
      defaultMapRecordToInstance(dataImportEventPayload, mappingRules, mappingParameters);
      MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));
      return Future.succeededFuture();
    } catch (Exception e) {
      LOGGER.warn("Error during preparing and executing mapping:", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.add(instance, success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOGGER.info("Duplicated event received by InstanceId: {}. Ignoring...", instance.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Instance id: %s", instance.getId())));
        } else {
          LOGGER.error(format("Error posting Instance by instanceId:'%s' cause %s, status code %s", instance.getId(), failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void deleteInstance(String id, String jobExecutionId, InstanceCollection instanceCollection) {
    Promise<Void> promise = Promise.promise();
    instanceCollection.delete(id, success -> {
        LOGGER.info("deleteInstance:: Instance was deleted by id: '{}', jobExecutionId: '{}'", id, jobExecutionId);
        promise.complete(success.getResult());
      },
      failure -> {
        LOGGER.warn("deleteInstance:: Error deleting Instance by id: '{}', jobExecutionId: '{}', cause: {}, status code: {}",
          id, jobExecutionId, failure.getReason(), failure.getStatusCode());
        promise.fail(failure.getReason());
      });
    promise.future();
  }
}
