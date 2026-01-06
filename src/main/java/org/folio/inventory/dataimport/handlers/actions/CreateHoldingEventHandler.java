package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.consortium.util.ConsortiumUtil;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.logging.log4j.util.Strings.isNotEmpty;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateHoldingEventHandler.class);
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String HOLDINGS_PATH_FIELD = "holdings";
  private static final String CREATE_HOLDING_ERROR_MESSAGE = "Failed to create Holdings";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t create Holding entity: context is empty or doesn`t exists";
  private static final String PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG = "Failed to extract instanceId from instance entity or parsed record";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create a Holding entity requires a mapping profile";
  private static final String FOLIO_SOURCE_ID = "f32d531e-df79-46b3-8932-cdd35f7a2264";
  private static final String ERRORS = "ERRORS";
  private static final String BLANK = "";
  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;
  private final IdStorageService idStorageService;
  private final OrderHelperService orderHelperService;
  private final ConsortiumService consortiumService;

  public CreateHoldingEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache, IdStorageService idStorageService,
                                   OrderHelperService orderHelperServiceImpl, ConsortiumService consortiumService) {
    this.orderHelperService = orderHelperServiceImpl;
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
    this.idStorageService = idStorageService;
    this.consortiumService = consortiumService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
        || StringUtils.isEmpty(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))) {
        LOGGER.warn("handle:: Can`t create Holding entity for context: {} jobExecutionId: {}", payloadContext, jobExecutionId);
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      String recordId = payloadContext.get(RECORD_ID_HEADER);
      String chunkId = payloadContext.get(CHUNK_ID_HEADER);
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.warn("handle:: {} jobExecutionId: {} recordId: {}", ACTION_HAS_NO_MAPPING_MSG, jobExecutionId, recordId);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(),
        payloadContext.get(PAYLOAD_USER_ID), payloadContext.get(OKAPI_REQUEST_ID));
      LOGGER.info("handle:: Create holding with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

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
              JsonArray holdingsList = new JsonArray(payloadContext.get(HOLDINGS.value()));
              String instanceId = getInstanceId(dataImportEventPayload);
              for (int i = 0; i < holdingsList.size(); i++) {
                JsonObject holdingAsJson = holdingsList.getJsonObject(i);
                if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
                  holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
                  holdingsList.set(i, holdingAsJson);
                }
                holdingAsJson.put("id", (i == 0) ? holdingsId : UUID.randomUUID().toString());
                holdingAsJson.put("sourceId", FOLIO_SOURCE_ID);
                fillInstanceIdIfNeeded(instanceId, holdingAsJson);
              }

              LOGGER.trace(format("handle:: Mapped holdings: %s", holdingsList.encode()));
              dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingsList.encode());
              return List.of(Json.decodeValue(payloadContext.get(HOLDINGS.value()), HoldingsRecord[].class));
            })
            .compose(holdingsToCreate -> consortiumService.getConsortiumConfiguration(context)
              .compose(consortiumConfigurationOptional -> {
                if (consortiumConfigurationOptional.isPresent()) {
                  return ConsortiumUtil.createShadowInstanceIfNeeded(consortiumService, storage.getInstanceCollection(context),
                      context, getInstanceId(dataImportEventPayload), consortiumConfigurationOptional.get())
                    .map(holdingsToCreate);
                }
                return Future.succeededFuture(holdingsToCreate);
              }))
            .compose(holdingsToCreate -> addHoldings(holdingsToCreate, payloadContext, context))
            .onSuccess(createdHoldings -> {
              LOGGER.info("handle:: Created Holdings records by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}'",
                jobExecutionId, recordId, chunkId);
              payloadContext.put(HOLDINGS.value(), Json.encode(createdHoldings));
              orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload, DI_INVENTORY_HOLDING_CREATED, context)
                .onComplete(result -> future.complete(dataImportEventPayload));
            })
            .onFailure(e -> {
              if (!(e instanceof DuplicateEventException)) {
                LOGGER.warn("handle:: Error creating inventory Holding record by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
                  recordId, chunkId, e);
              }
              future.completeExceptionally(e);
            });
        })
        .onFailure(failure -> {
          LOGGER.warn("handle:: Error creating inventory recordId and holdingsId relationship by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ",
            jobExecutionId, recordId, chunkId, failure);
          future.completeExceptionally(failure);
        });
    } catch (Exception e) {
      LOGGER.warn("handle:: " + CREATE_HOLDING_ERROR_MESSAGE, e);
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
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonArray().encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().getFirst());
  }

  private void fillInstanceIdIfNeeded(String instanceId, JsonObject holdingAsJson) {
    if (isBlank(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      if (isBlank(instanceId)) {
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG);
      }
      fillInstanceId(holdingAsJson, instanceId);
    }
  }

  private static String getInstanceId(DataImportEventPayload dataImportEventPayload) {
    String instanceId = null;
    String instanceAsString = dataImportEventPayload.getContext().get(EntityType.INSTANCE.value());

    if (isNotEmpty(instanceAsString)) {
      JsonObject instanceRecord = new JsonObject(instanceAsString);
      instanceId = instanceRecord.getString("id");
    }
    if (isBlank(instanceId)) {
      String recordAsString = dataImportEventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value());
      Record record = Json.decodeValue(recordAsString, Record.class);
      instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    }
    return instanceId;
  }

  private void fillInstanceId(JsonObject holdingAsJson, String instanceId) {
    LOGGER.trace(format("fillInstanceId:: Adding instance id: %s, to holding with id: %s", instanceId, holdingAsJson.getString("id")));
    holdingAsJson.put(INSTANCE_ID_FIELD, instanceId);
  }

  private Future<List<HoldingsRecord>> addHoldings(List<HoldingsRecord> holdingsList, HashMap<String, String> payloadContext, Context context) {
    Promise<List<HoldingsRecord>> holdingsPromise = Promise.promise();
    List<HoldingsRecord> createdHoldingsRecord = new ArrayList<>();
    List<PartialError> errors = new ArrayList<>();
    List<Future<?>> createHoldingsRecordFutures = new ArrayList<>();

    HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
    holdingsList.forEach(holdings -> {
      LOGGER.debug(format("addHoldings:: Trying to add holdings with id: %s", holdings.getId()));
      Promise<Void> createPromise = Promise.promise();
      createHoldingsRecordFutures.add(createPromise.future());
      holdingsRecordCollection.add(holdings,
        success -> {
          createdHoldingsRecord.add(success.getResult());
          createPromise.complete();
        },
        failure -> {
          errors.add(new PartialError(holdings.getId() != null ? holdings.getId() : BLANK, failure.getReason()));
          if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
            LOGGER.info("addHoldings:: Duplicated event received by Holding id: {}. Ignoring...", holdings.getId());
            createPromise.fail(new DuplicateEventException(format("Duplicated event by Holding id: %s", holdings.getId())));
          } else {
            LOGGER.warn(format("addHoldings:: Error posting Holdings cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
            createPromise.complete();
          }
        });
    });
    Future.all(createHoldingsRecordFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        String errorsAsStringJson = Json.encode(errors);
        if (!createdHoldingsRecord.isEmpty()) {
          payloadContext.put(ERRORS, errorsAsStringJson);
          holdingsPromise.complete(createdHoldingsRecord);
        } else {
          holdingsPromise.fail(errorsAsStringJson);
        }
      } else {
        holdingsPromise.fail(ar.cause());
      }
    });
    return holdingsPromise.future();
  }

  @Getter
  private static class HoldingsCreationException extends RuntimeException {
    private final PartialError error;

    public HoldingsCreationException(PartialError error) {
      super(error.getError());
      this.error = error;
    }

  }
}
