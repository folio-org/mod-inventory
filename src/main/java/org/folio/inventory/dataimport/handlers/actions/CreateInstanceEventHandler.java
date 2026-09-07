package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_999;
import static org.folio.dataimport.util.marc.MarcConstants.INDICATOR_F;
import static org.folio.dataimport.util.marc.MarcConstants.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.DataImportConstants.ALREADY_EXISTS_ERROR_MSG;
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
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

public class CreateInstanceEventHandler extends AbstractInstanceEventHandler {

  static final String ACTION_HAS_NO_MAPPING_MSG =
    "Action profile to create an Instance requires a mapping profile by jobExecutionId: '%s' and recordId: '%s'";
  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceEventHandler.class);
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Failed to handle event payload - event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG =
    "MappingParameters snapshot was not found by jobExecutionId: '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String INSTANCE_CREATION_999_ERROR_MESSAGE =
    "A new Instance was not created because the incoming record already contains a 999ff$s or 999ff$i field";
  protected final IdStorageService idStorageService;
  private final OrderHelperService orderHelperService;

  public CreateInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                    MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService,
                                    OrderHelperService orderHelperService, SnapshotService snapshotService,
                                    HttpClient httpClient) {
    super(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, snapshotService, httpClient);
    this.orderHelperService = orderHelperService;
    this.idStorageService = idStorageService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    logParametersEventHandler(LOGGER, payload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      payload.setEventType(DI_INVENTORY_INSTANCE_CREATED.value());

      HashMap<String, String> payloadContext = payload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
          || isEmpty(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }

      String jobExecutionId = payload.getJobExecutionId();
      String recordId = payloadContext.get(DataImportHeaders.RECORD_ID);
      if (payload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error("handle:: {} jobExecutionId: {} recordId: {}", ACTION_HAS_NO_MAPPING_MSG, jobExecutionId,
          recordId);
        return CompletableFuture.failedFuture(
          new EventProcessingException(format(ACTION_HAS_NO_MAPPING_MSG, jobExecutionId, recordId)));
      }

      Context context = buildContext(payload, payloadContext);
      Record targetRecord = Json.decodeValue(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()), Record.class);
      String sourceContent = targetRecord.getParsedRecord().getContent().toString();

      if (!Boolean.parseBoolean(payloadContext.get("acceptInstanceId")) && contains999ffSubfieldI(targetRecord)) {
        LOGGER.error("handle:: {} jobExecutionId: {} recordId: {} ", INSTANCE_CREATION_999_ERROR_MESSAGE,
          jobExecutionId, recordId);
        return CompletableFuture.failedFuture(new EventProcessingException(INSTANCE_CREATION_999_ERROR_MESSAGE));
      }

      String chunkId = payloadContext.get(DataImportHeaders.CHUNK_ID);
      LOGGER.info("Create instance with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId,
        chunkId);

      storeAndProcess(targetRecord, sourceContent, payload, payloadContext, context, jobExecutionId, recordId, chunkId,
        future);
    } catch (Exception e) {
      LOGGER.error("Error creating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode()
      .getContentType()) {
      ActionProfile actionProfile =
        JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value();
  }

  protected Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.add(instance, success -> promise.complete(success.result()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.reason()) && failure.reason()
          .contains(String.format(ALREADY_EXISTS_ERROR_MSG, instance.getId()))) {
          LOGGER.info("Duplicated event received by InstanceId: {}. Ignoring...", instance.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Instance id: %s", instance.getId())));
        } else {
          LOGGER.error("Error posting Instance by instanceId:'{}' cause {}, status code {}", instance.getId(),
            failure.reason(), failure.statusCode());
          promise.fail(failure.reason());
        }
      });
    return promise.future();
  }

  private Context buildContext(DataImportEventPayload payload, HashMap<String, String> payloadContext) {
    return EventHandlingUtil.constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl(),
      payloadContext.get(DataImportHeaders.USER_ID), payloadContext.get(XOkapiHeaders.REQUEST_ID.toLowerCase()));
  }

  private void storeAndProcess(Record targetRecord, String sourceContent, DataImportEventPayload payload,
                               HashMap<String, String> payloadContext, Context context,
                               String jobExecutionId, String recordId, String chunkId,
                               CompletableFuture<DataImportEventPayload> future) {
    idStorageService.store(targetRecord.getId(), super.getInstanceId(targetRecord), payload.getTenant())
      .onSuccess(res -> {
        String instanceId = res.getEntityId();
        createInstance(instanceId, payload, payloadContext, context, targetRecord, sourceContent,
          jobExecutionId, recordId, chunkId)
          .onSuccess(createdInstance -> completeWithOrderProcessing(createdInstance, payload, context, future))
          .onFailure(e -> {
            if (!(e instanceof DuplicateEventException)) {
              LOGGER.error(
                "Error creating inventory Instance by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ",
                jobExecutionId, recordId, chunkId, e);
            }
            future.completeExceptionally(e);
          });
      })
      .onFailure(failure -> {
        LOGGER.error("Error creating inventory recordId and instanceId relationship by jobExecutionId: '{}' "
                     + "and recordId: '{}' and chunkId: '{}' ", jobExecutionId, recordId, chunkId, failure);
        future.completeExceptionally(failure);
      });
  }

  private void completeWithOrderProcessing(Instance createdInstance, DataImportEventPayload payload,
                                           Context context, CompletableFuture<DataImportEventPayload> future) {
    payload.getContext().put(INSTANCE.value(), Json.encode(createdInstance));
    orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(payload, DI_INVENTORY_INSTANCE_CREATED, context)
      .onComplete(result -> future.complete(payload));
  }

  private Future<Instance> createInstance(String instanceId, DataImportEventPayload payload,
                                          HashMap<String, String> payloadContext, Context context, Record targetRecord,
                                          String sourceContent, String jobExecutionId, String recordId,
                                          String chunkId) {
    InstanceCollection instanceCollection = storage.getInstanceCollection(context);
    return getMappingMetadataCache().get(jobExecutionId, context)
      .compose(parametersOptional -> parametersOptional
        .map(mappingMetadata -> applyFieldsManipulationAndMap(mappingMetadata, targetRecord, payload, payloadContext))
        .orElseGet(
          () -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId, recordId, chunkId))))
      .compose(v -> buildAndPersistInstance(instanceId, payload, context, targetRecord, sourceContent,
        instanceCollection, jobExecutionId, recordId, chunkId));
  }

  private Future<Void> applyFieldsManipulationAndMap(MappingMetadataDto mappingMetadata, Record targetRecord,
                                                     DataImportEventPayload payload,
                                                     HashMap<String, String> payloadContext) {
    MappingParameters mappingParameters =
      Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
    AdditionalFieldsUtil.executeStandardFieldsManipulation(targetRecord, mappingParameters, Clock.systemDefaultZone());
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(targetRecord));
    return prepareAndExecuteMapping(payload, new JsonObject(mappingMetadata.getMappingRules()), mappingParameters);
  }

  private Future<Instance> buildAndPersistInstance(String instanceId, DataImportEventPayload payload,
                                                   Context context, Record targetRecord, String sourceContent,
                                                   InstanceCollection instanceCollection,
                                                   String jobExecutionId, String recordId, String chunkId) {
    JsonObject instanceAsJson = prepareInstance(payload, instanceId, jobExecutionId);

    List<String> requiredFieldsErrors =
      EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, INSTANCE_REQUIRED_FIELDS);
    if (!requiredFieldsErrors.isEmpty()) {
      return failWithInvalidInstanceMsg(requiredFieldsErrors, jobExecutionId, recordId, chunkId);
    }

    Instance mappedInstance = Instance.fromJson(instanceAsJson);

    List<String> invalidUuidsErrors = ValidationUtil.validateUuids(mappedInstance);
    if (!invalidUuidsErrors.isEmpty()) {
      return failWithInvalidInstanceMsg(invalidUuidsErrors, jobExecutionId, recordId, chunkId);
    }

    markInstanceAndRecordAsDeletedIfNeeded(mappedInstance, targetRecord);
    return persistInstance(mappedInstance, instanceCollection, context, targetRecord, sourceContent, payload);
  }

  private <T> Future<T> failWithInvalidInstanceMsg(List<String> errors,
                                                   String jobExecutionId, String recordId, String chunkId) {
    String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ",
      errors, jobExecutionId, recordId, chunkId);
    LOGGER.warn(msg);
    return Future.failedFuture(msg);
  }

  private Future<Instance> persistInstance(Instance instance, InstanceCollection instanceCollection,
                                           Context context, Record targetRecord, String sourceContent,
                                           DataImportEventPayload payload) {
    return addInstance(instance, instanceCollection)
      .compose(createdInstance -> getPrecedingSucceedingTitlesHelper()
        .createPrecedingSucceedingTitles(instance, context)
        .map(createdInstance))
      .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord))
      .compose(createdInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var reorderedContent = reorderMarcRecordFields(sourceContent, targetContent, targetRecord.getId());
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(reorderedContent));
        setSuppressFromDiscovery(targetRecord, createdInstance.getDiscoverySuppress());
        return saveRecordInSrsAndHandleResponse(payload, targetRecord, createdInstance, instanceCollection,
          payload.getTenant(), context.getUserId(), context.getRequestId());
      });
  }

  private boolean contains999ffSubfieldI(Record targetRecord) {
    return AdditionalFieldsUtil.getValueFromDataField(targetRecord, FIELD_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I)
      .isPresent();
  }

  private JsonObject prepareInstance(DataImportEventPayload dataImportEventPayload, String instanceId,
                                     String jobExecutionId) {
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

  private Future<Void> prepareAndExecuteMapping(DataImportEventPayload dataImportEventPayload, JsonObject mappingRules,
                                                MappingParameters mappingParameters) {
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
}
