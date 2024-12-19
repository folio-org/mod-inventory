package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.getTenant;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

public class MarcBibModifiedPostProcessingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibModifiedPostProcessingEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Event does not contain required data to update Instance";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final MappingMetadataCache mappingMetadataCache;

  public MarcBibModifiedPostProcessingEventHandler(InstanceUpdateDelegate updateInstanceDelegate, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                                   MappingMetadataCache mappingMetadataCache) {
    this.instanceUpdateDelegate = updateInstanceDelegate;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }

      LOGGER.info("Processing starting with jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());

      Record record = new JsonObject(payloadContext.get(MARC_BIBLIOGRAPHIC.value())).mapTo(Record.class);
      String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      if (isBlank(instanceId)) {
        return CompletableFuture.completedFuture(dataImportEventPayload);
      }

      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
      Context localTenantContext = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(), payloadContext.get(EventHandlingUtil.USER_ID));
      Context targetInstanceContext = EventHandlingUtil.constructContext(getTenant(dataImportEventPayload), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(), payloadContext.get(EventHandlingUtil.USER_ID));
      Promise<Instance> instanceUpdatePromise = Promise.promise();

      mappingMetadataCache.get(dataImportEventPayload.getJobExecutionId(), localTenantContext)
        .map(parametersOptional -> parametersOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, dataImportEventPayload.getJobExecutionId()))))
        .map(mappingMetadataDto -> buildPayloadForInstanceUpdate(dataImportEventPayload, mappingMetadataDto))
        .compose(payloadForUpdate -> instanceUpdateDelegate.handle(payloadForUpdate, record, targetInstanceContext))
        .onSuccess(instanceUpdatePromise::complete)
        .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(updatedInstance, targetInstanceContext))
        .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
          .map(titleJson -> titleJson.getString("id"))
          .collect(Collectors.toSet()))
        .compose(precedingSucceedingTitles -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(precedingSucceedingTitles, targetInstanceContext))
        .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(instanceUpdatePromise.future().result(), targetInstanceContext))
        .onComplete(updateAr -> {
          if (updateAr.succeeded()) {
            dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
            Instance resultedInstance = instanceUpdatePromise.future().result();
            if (resultedInstance.getVersion() != null) {
              int currentVersion = Integer.parseInt(resultedInstance.getVersion());
              int incrementedVersion = currentVersion + 1;
              resultedInstance.setVersion(String.valueOf(incrementedVersion));
            }
            dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(resultedInstance));
            future.complete(dataImportEventPayload);
          } else {
            if (updateAr.cause() instanceof OptimisticLockingException) {
              processOLError(dataImportEventPayload, future, updateAr);
            } else {
              dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
              LOGGER.error("Error updating inventory instance by id: '{}' by jobExecutionId: '{}'", instanceId, dataImportEventPayload.getJobExecutionId(), updateAr.cause());
              future.completeExceptionally(updateAr.cause());
            }
          }
        });
    } catch (Exception e) {
      dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      LOGGER.error("Error updating inventory instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void processOLError(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, AsyncResult<Void> updateAr) {
    int currentRetryNumber = dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER) == null
      ? 0 : Integer.parseInt(dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      dataImportEventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Instance - {}. Retry MarcBibModifiedPostProcessingEventHandler handler...", updateAr.cause().getMessage());
      handle(dataImportEventPayload).whenComplete((res, e) -> {
        if (e != null) {
          future.completeExceptionally(e);
        } else {
          future.complete(dataImportEventPayload);
        }
      });
    } else {
      dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded given number %s for the Instance update", MAX_RETRIES_COUNT, currentRetryNumber);
      LOGGER.error(errMessage);
      future.completeExceptionally(new OptimisticLockingException(errMessage));
    }
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MAPPING_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MappingProfile mappingProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MappingProfile.class);
      return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value().equals(dataImportEventPayload.getEventType())
        && mappingProfile.getExistingRecordType() == EntityType.MARC_BIBLIOGRAPHIC;
    }
    return false;
  }

  private Map<String, String> buildPayloadForInstanceUpdate(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadataDto) {
    HashMap<String, String> preparedPayload = new HashMap<>(dataImportEventPayload.getContext());
    preparedPayload.put(MAPPING_RULES_KEY, mappingMetadataDto.getMappingRules());
    preparedPayload.put(MAPPING_PARAMS_KEY, mappingMetadataDto.getMappingParams());
    return preparedPayload;
  }
}
