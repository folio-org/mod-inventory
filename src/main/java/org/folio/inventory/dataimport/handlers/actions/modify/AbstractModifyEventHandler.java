package org.folio.inventory.dataimport.handlers.actions.modify;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.*;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.getTenant;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

public abstract class AbstractModifyEventHandler implements EventHandler {
  private static final Logger LOGGER = LogManager.getLogger(AbstractModifyEventHandler.class);
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Failed to handle event payload, cause event payload context does not contain required data to modify MARC record";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String FAILED_TO_UPDATE_RECORD_ERROR_MESSAGE = "Failed to update MARC record with id: %s during modify for tenant %s. ";
  private final MappingMetadataCache mappingMetadataCache;
  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final HttpClient client;

  protected AbstractModifyEventHandler(MappingMetadataCache mappingMetadataCache, InstanceUpdateDelegate instanceUpdateDelegate, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper, HttpClient client) {
    this.mappingMetadataCache = mappingMetadataCache;
    this.instanceUpdateDelegate = instanceUpdateDelegate;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.client = client;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    logParametersEventHandler(LOGGER, payload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = payload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(modifiedEntityType().value()))) {
        LOGGER.warn(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      LOGGER.info("handle:: Processing {} modifying starting with jobExecutionId: {}.", modifiedEntityType(), payload.getJobExecutionId());
      Context localTenantContext = constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl(), payloadContext.get(PAYLOAD_USER_ID));

      mappingMetadataCache.get(payload.getJobExecutionId(), localTenantContext)
        .map(mapMappingMetaDataOrFail(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, payload.getJobExecutionId())))
        .compose(mappingMetadataDto -> modifyRecord(payload, getMappingParameters(mappingMetadataDto)).map(mappingMetadataDto))
        .compose(mappingMetadataDto -> {
          if (payloadContext.containsKey(relatedEntityType().value())) {
            Context targetInstanceContext = constructContext(getTenant(payload), payload.getToken(), payload.getOkapiUrl(), payloadContext.get(PAYLOAD_USER_ID));
            return updateRelatedEntity(payload, mappingMetadataDto, targetInstanceContext)
              .compose(v -> updateRecord(getRecord(payload.getContext()), targetInstanceContext));
          }
          return Future.succeededFuture();
        })
        .onSuccess(v -> submitSuccessfulEventType(payload, future))
        .onFailure(throwable -> {
          LOGGER.warn("handle:: Error while MARC record modifying", throwable);
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      payload.getContext().remove(CURRENT_RETRY_NUMBER);
      LOGGER.error("handle:: Error during modifying: {}", modifiedEntityType(), e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (payload.getCurrentNode() != null && ACTION_PROFILE == payload.getCurrentNode().getContentType()) {
      var actionProfile = JsonObject.mapFrom(payload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return isEligibleActionProfile(actionProfile);
    }
    return false;
  }

  protected boolean isEligibleActionProfile(ActionProfile actionProfile) {
    return actionProfile.getFolioRecord() == ActionProfile.FolioRecord.valueOf(modifiedEntityType().value())
      && actionProfile.getAction() == MODIFY;
  }

  protected abstract EntityType modifiedEntityType();

  protected abstract EntityType relatedEntityType();

  protected abstract String modifyEventType();

  protected Future<Void> modifyRecord(DataImportEventPayload payload, MappingParameters mappingParameters) {
    try {
      preparePayload(payload);
      MappingProfile mappingProfile = retrieveMappingProfile(payload);
      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(payload, mappingParameters, mappingProfile, modifiedEntityType());
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(payload);
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
    return Future.succeededFuture();
  }

  protected Future<Void> updateRelatedEntity(DataImportEventPayload payload, MappingMetadataDto mappingMetadataDto, Context context) {
    Promise<Void> promise = Promise.promise();
    Map<String, String> payloadForInstanceUpdate = buildPayloadForInstanceUpdate(payload, mappingMetadataDto);

    Record record = getRecord(payloadForInstanceUpdate);
    String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    if (isBlank(instanceId)) {
      LOGGER.warn("updateRelatedEntity:: Cannot update Instance during modify, 999ff$i is blank, tenant: {}, jobExecutionId: {}",
        context.getTenantId(), payload.getJobExecutionId());
      return Future.succeededFuture();
    }

    record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    Promise<Instance> instanceUpdatePromise = Promise.promise();

    instanceUpdateDelegate.handle(payloadForInstanceUpdate, record, context)
      .onSuccess(instanceUpdatePromise::complete)
      .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(updatedInstance, context))
      .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
        .map(titleJson -> titleJson.getString("id"))
        .collect(Collectors.toSet()))
      .compose(precedingSucceedingTitles -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(precedingSucceedingTitles, context))
      .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(instanceUpdatePromise.future().result(), context))
      .onSuccess(updateAr -> {
        LOGGER.debug("updateRelatedEntity:: Instance with id: '{}' successfully updated by jobExecutionId: '{}'", instanceId, payload.getJobExecutionId());
        payload.getContext().remove(CURRENT_RETRY_NUMBER);
        Instance resultedInstance = instanceUpdatePromise.future().result();
        if (resultedInstance.getVersion() != null) {
          int currentVersion = Integer.parseInt(resultedInstance.getVersion());
          resultedInstance.setVersion(String.valueOf(++currentVersion));
        }
        payload.getContext().put(INSTANCE.value(), Json.encode(resultedInstance));
        promise.complete();
      })
      .onFailure(cause -> {
        if (cause instanceof OptimisticLockingException) {
          processOLError(payload, mappingMetadataDto, promise, cause, context);
        } else {
          payload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.warn("updateRelatedEntity:: Error updating inventory instance by id: '{}' by jobExecutionId: '{}'", instanceId, payload.getJobExecutionId(), cause);
          promise.fail(cause);
        }
      });
    return promise.future();
  }

  private Record getRecord(Map<String, String> payloadForInstanceUpdate) {
    return new JsonObject(payloadForInstanceUpdate.get(modifiedEntityType().value())).mapTo(Record.class);
  }

  private Function<Optional<MappingMetadataDto>, MappingMetadataDto> mapMappingMetaDataOrFail(String message) {
    return mappingMetadata -> mappingMetadata.orElseThrow(() -> new EventProcessingException(message));
  }

  private MappingParameters getMappingParameters(MappingMetadataDto mappingMetadataDto) {
    return Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
  }

  private void submitSuccessfulEventType(DataImportEventPayload payload, CompletableFuture<DataImportEventPayload> future) {
    payload.setEventType(modifyEventType());
    future.complete(payload);
  }

  private MappingProfile retrieveMappingProfile(DataImportEventPayload payload) {
    ProfileSnapshotWrapper mappingProfileWrapper = payload.getCurrentNode();
    return new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
  }

  private Map<String, String> buildPayloadForInstanceUpdate(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadataDto) {
    HashMap<String, String> preparedPayload = new HashMap<>(dataImportEventPayload.getContext());
    preparedPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    preparedPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
    return preparedPayload;
  }

  private void processOLError(DataImportEventPayload payload, MappingMetadataDto mappingMetadataDto,
                              Promise<Void> promise, Throwable cause, Context context) {
    int currentRetryNumber = payload.getContext().get(CURRENT_RETRY_NUMBER) == null
      ? 0 : Integer.parseInt(payload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      payload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("processOLError:: Error updating Instance - {}. Retry MarcBibModifiedPostProcessingEventHandler handler...", cause.getMessage());
      updateRelatedEntity(payload, mappingMetadataDto, context).onComplete(res -> {
        if (res.failed()) {
          promise.fail(res.cause());
        } else {
          promise.complete();
        }
      });
    } else {
      payload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("processOLError:: Current retry number %s exceeded given number %s for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber);
      LOGGER.warn(errMessage);
      promise.fail(new OptimisticLockingException(errMessage));
    }
  }

  protected Future<Void> updateRecord(Record record, Context context) {
    Promise<Void> promise = Promise.promise();
    getSourceStorageRecordsClient(context).putSourceStorageRecordsById(record.getId(), record).onComplete(response -> {
      if (response.succeeded()) {
        int statusCode = response.result().statusCode();
        if (statusCode == HttpStatus.SC_OK) {
          LOGGER.debug("updateRecord:: MARC record with id {} was successfully updated during modify for tenant {}",
            record.getId(), context.getTenantId());
          promise.complete();
        } else {
          String updateRecordErrorMessage = String.format(FAILED_TO_UPDATE_RECORD_ERROR_MESSAGE + "Error message: %s. Status code: %s",
            record.getId(), context.getTenantId(), response.result().statusMessage(), statusCode);
          LOGGER.warn(updateRecordErrorMessage);
          promise.fail(updateRecordErrorMessage);
        }
      } else {
        String updateRecordErrorMessage = String.format(FAILED_TO_UPDATE_RECORD_ERROR_MESSAGE + "Error message: %s.",
          record.getId(), context.getTenantId(), response.cause());
        LOGGER.warn(updateRecordErrorMessage);
        promise.fail(updateRecordErrorMessage);
      }
    });

    return promise.future();
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(Context context) {
    return new SourceStorageRecordsClient(context.getOkapiLocation(), context.getTenantId(), context.getToken(), client);
  }

  private void preparePayload(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }
}
