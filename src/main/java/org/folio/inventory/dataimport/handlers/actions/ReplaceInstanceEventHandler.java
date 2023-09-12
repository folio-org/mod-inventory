package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.Record;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.consortium.services.ConsortiumService;

import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.METADATA_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;


public class ReplaceInstanceEventHandler extends AbstractInstanceEventHandler { // NOSONAR

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC or INSTANCE data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Instance requires a mapping profile";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String MARC_INSTANCE_SOURCE = "MARC";
  private static final String INSTANCE_ID_TYPE = "INSTANCE";
  public static final String CENTRAL_TENANT_INSTANCE_UPDATED_FLAG = "CENTRAL_TENANT_INSTANCE_UPDATED";

  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final MappingMetadataCache mappingMetadataCache;
  private final HttpClient httpClient;

  private final ConsortiumService consortiumService;

  public ReplaceInstanceEventHandler(Storage storage,
                                     PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                     MappingMetadataCache mappingMetadataCache,
                                     HttpClient client,
                                     ConsortiumService consortiumService) {
    super(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.mappingMetadataCache = mappingMetadataCache;
    this.httpClient = client;
    this.consortiumService = consortiumService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) { // NOSONAR
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_UPDATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null
        || payloadContext.isEmpty()
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))
        || isEmpty(dataImportEventPayload.getContext().get(INSTANCE.value()))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }
      LOGGER.info("handle:: Processing ReplaceInstanceEventHandler starting with jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Instance instanceToUpdate = Instance.fromJson(new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value())));

      if (instanceToUpdate.getSource().equals(CONSORTIUM_FOLIO.getValue()) || instanceToUpdate.getSource().equals(CONSORTIUM_MARC.getValue())) {
        LOGGER.error("!!!!");

        consortiumService.getConsortiumConfiguration(context)
          .compose(consortiumConfigurationOptional -> {
            if (consortiumConfigurationOptional.isPresent()) {
              Context centralTenantContext = EventHandlingUtil.constructContext(consortiumConfigurationOptional.get().getCentralTenantId(), context.getToken(), context.getOkapiLocation());
              InstanceCollection instanceCollection = storage.getInstanceCollection(centralTenantContext);
              getInstanceById(instanceToUpdate.getId(), instanceCollection)
                .onSuccess(existedCentralTenantInstance -> {
                  LOGGER.error("!!!!!Instance: {}", existedCentralTenantInstance);
                  LOGGER.error("!!!!!InstanceAsString: {}", existedCentralTenantInstance.toString());
                  processInstanceUpdate(dataImportEventPayload, instanceCollection, context, existedCentralTenantInstance, future, centralTenantContext.getTenantId());
                  dataImportEventPayload.getContext().put(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG, "true");
                })
                .onFailure(e -> {
                  LOGGER.warn("Error retrieving inventory Instance from central tenant", e);
                  future.completeExceptionally(e);
                });
            } else {
              LOGGER.warn("handle:: Can't retrieve centralTenantId updating Instance by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}'", dataImportEventPayload.getJobExecutionId(),
                dataImportEventPayload.getContext().get(RECORD_ID_HEADER), dataImportEventPayload.getContext().get(CHUNK_ID_HEADER));
              future.completeExceptionally(new NotFoundException("Can't retrieve centralTenantId updating Instance"));
            }
            return Future.succeededFuture();
          });

      } else {
        LOGGER.error("!!!!?");

        InstanceCollection instanceCollection = storage.getInstanceCollection(context);
        processInstanceUpdate(dataImportEventPayload, instanceCollection, context, instanceToUpdate, future, context.getTenantId());
      }
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void processInstanceUpdate(DataImportEventPayload dataImportEventPayload, InstanceCollection instanceCollection, Context context, Instance instanceToUpdate, CompletableFuture<DataImportEventPayload> future, String centralTenantId) {
    prepareEvent(dataImportEventPayload);

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();

    String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
    String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
    LOGGER.info("Replace instance with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

    mappingMetadataCache.get(jobExecutionId, context)
      .compose(parametersOptional -> parametersOptional
        .map(mappingMetadata -> prepareAndExecuteMapping(dataImportEventPayload, mappingMetadata, instanceToUpdate, centralTenantId))
        .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId,
          recordId, chunkId))))
      .compose(e -> {
        JsonObject instanceAsJson = prepareTargetInstance(dataImportEventPayload, instanceToUpdate);
        List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);

        if (!errors.isEmpty()) {
          String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", errors,
            jobExecutionId, recordId, chunkId);
          LOGGER.warn(msg);
          return Future.failedFuture(msg);
        }

        Instance mappedInstance = Instance.fromJson(instanceAsJson);
        return updateInstanceAndRetryIfOlExists(mappedInstance, instanceCollection, dataImportEventPayload)
          .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(mappedInstance, context))
          .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
            .map(titleJson -> titleJson.getString("id"))
            .collect(Collectors.toSet()))
          .compose(titlesIds -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(titlesIds, context))
          .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(mappedInstance, context))
          .map(instanceAsJson);
      })
      .onComplete(ar -> {
        if (ar.succeeded()) {
          dataImportEventPayload.getContext().put(INSTANCE.value(), ar.result().encode());
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          future.complete(dataImportEventPayload);
        } else {
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Error updating inventory Instance by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s': %s ", jobExecutionId,
            recordId, chunkId, ar.cause());
          LOGGER.error(errMessage);
          future.completeExceptionally(ar.cause());
        }
      });
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value();
  }


  private JsonObject prepareTargetInstance(DataImportEventPayload dataImportEventPayload, Instance instanceToUpdate) {
    JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
    if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
      instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
    }

    Set<String> precedingSucceedingIds = new HashSet<>();
    precedingSucceedingIds.addAll(instanceToUpdate.getPrecedingTitles()
      .stream()
      .filter(pr -> isNotEmpty(pr.id))
      .map(pr -> pr.id)
      .collect(Collectors.toList()));
    precedingSucceedingIds.addAll(instanceToUpdate.getSucceedingTitles()
      .stream()
      .filter(pr -> isNotEmpty(pr.id))
      .map(pr -> pr.id)
      .collect(Collectors.toList()));
    instanceAsJson.put("id", instanceToUpdate.getId());
    instanceAsJson.put(HRID_KEY, instanceToUpdate.getHrid());
    if (!(instanceToUpdate.getSource().equals(CONSORTIUM_FOLIO.getValue()) || instanceToUpdate.getSource().equals(CONSORTIUM_MARC.getValue()))) {
      instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
    }
    instanceAsJson.put(METADATA_KEY, instanceToUpdate.getMetadata());
    return instanceAsJson;
  }

  private Future<Void> prepareAndExecuteMapping(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadata, Instance instanceToUpdate, String tenantId) {
    JsonObject mappingRules = new JsonObject(mappingMetadata.getMappingRules());
    MappingParameters mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);

    return prepareRecordForMapping(dataImportEventPayload, mappingParameters.getMarcFieldProtectionSettings(), instanceToUpdate, tenantId)
      .onSuccess(v -> {
        org.folio.Instance mapped = defaultMapRecordToInstance(dataImportEventPayload, mappingRules, mappingParameters);
        Instance mergedInstance = InstanceUtil.mergeFieldsWhichAreNotControlled(instanceToUpdate, mapped);
        dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(mergedInstance))));
        MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));
      });
  }

  private Future<Void> prepareRecordForMapping(DataImportEventPayload dataImportEventPayload,
                                               List<MarcFieldProtectionSetting> marcFieldProtectionSettings,
                                               Instance instance, String tenantId) {
    LOGGER.error("SOURCE!!!: {}", instance.getSource());
    if (!MARC_INSTANCE_SOURCE.equals(instance.getSource()) && !CONSORTIUM_MARC.getValue().equals(instance.getSource())) {
      LOGGER.error("INSIDE!!!: ");
      return Future.succeededFuture();
    }

    return getRecordByInstanceId(dataImportEventPayload, instance.getId(), tenantId)
      .compose(existingRecord -> {
        Record incomingRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
        String updatedContent = new MarcRecordModifier().updateRecord(incomingRecord, existingRecord, marcFieldProtectionSettings);
        incomingRecord.getParsedRecord().setContent(updatedContent);
        dataImportEventPayload.getContext().put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
        return Future.succeededFuture();
      });
  }

  private Future<Record> getRecordByInstanceId(DataImportEventPayload dataImportEventPayload,
                                               String instanceId, String tenantId) {
    SourceStorageRecordsClient client = new SourceStorageRecordsClient(dataImportEventPayload.getOkapiUrl(),
      tenantId, dataImportEventPayload.getToken(), httpClient);

    return client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE).compose(resp -> {
      if (resp.statusCode() != 200) {
        return Future.failedFuture(format("Failed to retrieve MARC record by instance id: '%s', status code: %s",
          instanceId, resp.statusCode()));
      }
      return Future.succeededFuture(resp.bodyAsJson(Record.class));
    });
  }

  public Future<Instance> updateInstanceAndRetryIfOlExists(Instance instance, InstanceCollection instanceCollection, DataImportEventPayload eventPayload) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          processOLError(instance, instanceCollection, eventPayload, promise, failure);
        } else {
          eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void processOLError(Instance instance, InstanceCollection instanceCollection, DataImportEventPayload eventPayload, Promise<Instance> promise, Failure failure) {
    int currentRetryNumber = eventPayload.getContext().get(CURRENT_RETRY_NUMBER) == null ? 0 : Integer.parseInt(eventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      eventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("OL error updating Instance - {}, status code {}. Retry ReplaceInstanceEventHandler handler...", failure.getReason(), failure.getStatusCode());
      getActualInstanceAndReInvokeCurrentHandler(instance, instanceCollection, promise, eventPayload);
    } else {
      eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Instance update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, eventPayload.getJobExecutionId());
      LOGGER.error(errMessage);
      promise.fail(errMessage);
    }
  }

  private void getActualInstanceAndReInvokeCurrentHandler(Instance instance, InstanceCollection instanceCollection, Promise<Instance> promise, DataImportEventPayload eventPayload) {
    instanceCollection.findById(instance.getId())
      .thenAccept(actualInstance -> {
        eventPayload.getContext().put(INSTANCE.value(), Json.encode(JsonObject.mapFrom(actualInstance)));
        eventPayload.getEventsChain().remove(eventPayload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
        eventPayload.setCurrentNode(Json.decodeValue(eventPayload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
        eventPayload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
        eventPayload.getContext().remove(CURRENT_NODE_PROPERTY);

        handle(eventPayload).whenComplete((res, e) -> {
          if (e != null) {

            promise.fail(e.getMessage());
          } else {
            promise.complete();
          }
        });
      })
      .exceptionally(e -> {
        eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
        LOGGER.error("Cannot get actual Instance by id: {}", instance.getId(), e);
        promise.fail(format("Cannot get actual Instance by id: %s, cause: %s", instance.getId(), e.getMessage()));
        return null;
      });
  }

  private Future<Instance> getInstanceById(String instanceId, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.error("Can't find Instance by id: {} ", instanceId);
          promise.fail(new NotFoundException(format("Can't find Instance by id: %s", instanceId)));
        } else {
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
