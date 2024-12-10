package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_PATH;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.ID;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
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
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

public class CreateInstanceEventHandler extends AbstractInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload - event payload context does not contain MARC_BIBLIOGRAPHIC data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create an Instance requires a mapping profile by jobExecutionId: '%s' and recordId: '%s'";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId: '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String INSTANCE_CREATION_999_ERROR_MESSAGE = "A new Instance was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  public static final String DISCOVERY_SUPPRESS_PROPERTY = "discoverySuppress";
  protected final IdStorageService idStorageService;
  private final OrderHelperService orderHelperService;

  public CreateInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                    MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService,
                                    OrderHelperService orderHelperService, HttpClient httpClient) {
    super(storage,precedingSucceedingTitlesHelper,mappingMetadataCache, httpClient);
    this.orderHelperService = orderHelperService;
    this.idStorageService = idStorageService;
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
      LOGGER.info("CreateInstanceEventHandler.handle:: context token: {} ", context.getToken());
      Record targetRecord = Json.decodeValue(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()), Record.class);
      var sourceContent = targetRecord.getParsedRecord().getContent().toString();

      if (!Boolean.parseBoolean(payloadContext.get("acceptInstanceId")) && AdditionalFieldsUtil.getValue(targetRecord, TAG_999, SUBFIELD_I).isPresent()) {
        LOGGER.error(INSTANCE_CREATION_999_ERROR_MESSAGE);
        return CompletableFuture.failedFuture(new EventProcessingException(INSTANCE_CREATION_999_ERROR_MESSAGE));
      }

      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      LOGGER.info("Create instance with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

      Future<RecordToEntity> recordToInstanceFuture = idStorageService.store(targetRecord.getId(), super.getInstanceId(targetRecord), dataImportEventPayload.getTenant());
      recordToInstanceFuture.onSuccess(res -> {
          String instanceId = res.getEntityId();
          getMappingMetadataCache().get(jobExecutionId, context)
            .compose(parametersOptional -> parametersOptional
              .map(mappingMetadata -> {
                MappingParameters mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
                AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
                AdditionalFieldsUtil.move001To035(targetRecord);
                AdditionalFieldsUtil.normalize035(targetRecord);
                payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(targetRecord));
                return prepareAndExecuteMapping(dataImportEventPayload, new JsonObject(mappingMetadata.getMappingRules()), mappingParameters);
              })
              .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId, recordId, chunkId))))
            .compose(v -> {
              InstanceCollection instanceCollection = storage.getInstanceCollection(context);
              JsonObject instanceAsJson = prepareInstance(dataImportEventPayload, instanceId, jobExecutionId);
              List<String> requiredFieldsErrors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, INSTANCE_REQUIRED_FIELDS);
              if (!requiredFieldsErrors.isEmpty()) {
                String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", requiredFieldsErrors,
                  jobExecutionId, recordId, chunkId);
                LOGGER.warn(msg);
                return Future.failedFuture(msg);
              }

              Instance mappedInstance = Instance.fromJson(instanceAsJson);

              List<String> invalidUUIDsErrors = ValidationUtil.validateUUIDs(mappedInstance);
              if (!invalidUUIDsErrors.isEmpty()) {
                String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", invalidUUIDsErrors,
                  jobExecutionId, recordId, chunkId);
                LOGGER.warn(msg);
                return Future.failedFuture(msg);
              }

              return addInstance(mappedInstance, instanceCollection)
                .compose(createdInstance -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(mappedInstance, context).map(createdInstance))
                .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord))
                .compose(createdInstance -> {
                  var targetContent = targetRecord.getParsedRecord().getContent().toString();
                  var content = reorderMarcRecordFields(sourceContent, targetContent);
                  targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
                  setSuppressFormDiscovery(targetRecord, instanceAsJson.getBoolean(DISCOVERY_SUPPRESS_PROPERTY, false));
                  return saveRecordInSrsAndHandleResponse(dataImportEventPayload, targetRecord, createdInstance, instanceCollection, dataImportEventPayload.getTenant());
                });
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

  private JsonObject prepareInstance(DataImportEventPayload dataImportEventPayload, String instanceId, String jobExecutionId) {
    JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
    if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
      instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
    }
    instanceAsJson.put(ID, instanceId);
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

  protected Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
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
}
