package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
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
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.METADATA_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ReplaceInstanceEventHandler extends AbstractInstanceEventHandler { // NOSONAR

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC or INSTANCE data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Instance requires a mapping profile";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));

  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final MappingMetadataCache mappingMetadataCache;

  public ReplaceInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper, MappingMetadataCache mappingMetadataCache) {
    super(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) { // NOSONAR
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

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Instance instanceToUpdate = Instance.fromJson(new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value())));

      prepareEvent(dataImportEventPayload);

      String jobExecutionId = dataImportEventPayload.getJobExecutionId();

      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);
      mappingMetadataCache.get(jobExecutionId, context)
        .compose(parametersOptional -> parametersOptional
          .map(mappingMetadata -> prepareAndExecuteMapping(dataImportEventPayload, new JsonObject(mappingMetadata.getMappingRules()), new JsonObject(mappingMetadata.getMappingParams())
            .mapTo(MappingParameters.class), instanceToUpdate))
          .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId,
            recordId, chunkId))))
        .compose(e -> {
          JsonObject instanceAsJson = prepareTargetInstance(dataImportEventPayload, instanceToUpdate);
          InstanceCollection instanceCollection = storage.getInstanceCollection(context);
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
            LOGGER.error("Error updating inventory Instance by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
              recordId, chunkId, ar.cause());
            future.completeExceptionally(ar.cause());
          }
        });
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
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
    instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
    instanceAsJson.put(METADATA_KEY, instanceToUpdate.getMetadata());
    return instanceAsJson;
  }

  private Future<Void> prepareAndExecuteMapping(DataImportEventPayload dataImportEventPayload, JsonObject mappingRules, MappingParameters mappingParameters, Instance instanceToUpdate) {
    try {
      org.folio.Instance mapped = defaultMapRecordToInstance(dataImportEventPayload, mappingRules, mappingParameters);
      Instance mergedInstance = InstanceUtil.mergeFieldsWhichAreNotControlled(instanceToUpdate, mapped);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(mergedInstance))));
      MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));
      return Future.succeededFuture();
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public Future<Instance> updateInstanceAndRetryIfOlExists(Instance instance, InstanceCollection instanceCollection, DataImportEventPayload eventPayload) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          String retry = eventPayload.getContext().get(CURRENT_RETRY_NUMBER);
          if (retry == null) {
            retry = "0";
          }
          int currentRetryNumber = Integer.parseInt(retry);
          if (currentRetryNumber < MAX_RETRIES_COUNT) {
            eventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
            LOGGER.warn(format("Error updating Instance - %s, status code %s. Retry InstanceUpdateDelegate handler...", failure.getReason(), failure.getStatusCode()));
            getActualInstanceAndReInvokeCurrentHandler(instance, instanceCollection, promise, eventPayload);
          } else {
            eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
            LOGGER.error("Current retry number {} exceeded or equal given number {} for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber);
            promise.fail(format("Current retry number %s exceeded or equal given number %s for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber));
          }
        } else {
          eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void getActualInstanceAndReInvokeCurrentHandler(Instance instance, InstanceCollection instanceCollection, Promise<Instance> promise, DataImportEventPayload eventPayload) {
    instanceCollection.findById(instance.getId())
      .thenAccept(actualInstance -> {
        eventPayload.getContext().put(INSTANCE.value(), Json.encode(JsonObject.mapFrom(actualInstance)));
        eventPayload.getEventsChain().remove(eventPayload.getContext().get("CURRENT_EVENT_TYPE"));
        try {
          eventPayload.setCurrentNode(ObjectMapperTool.getMapper().readValue(eventPayload.getContext().get("CURRENT_NODE"), ProfileSnapshotWrapper.class));
        } catch (JsonProcessingException e) {
          LOGGER.error(format("Cannot map from CURRENT_NODE value %s", e.getCause()));
        }
        eventPayload.getContext().remove("CURRENT_EVENT_TYPE");
        eventPayload.getContext().remove("CURRENT_NODE");

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
        LOGGER.error(format("Cannot get actual Instance by id: %s", e.getCause()));
        promise.fail(format("Cannot get actual Instance by id: %s", e.getCause()));
        return null;
      });
  }
}
