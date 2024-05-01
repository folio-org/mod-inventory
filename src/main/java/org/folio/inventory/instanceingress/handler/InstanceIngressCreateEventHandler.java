package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.instances.Instance.ID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.services.OrderHelperService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.Record;

public class InstanceIngressCreateEventHandler implements InstanceIngressEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressCreateEventHandler.class);
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload - event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String INSTANCE_CREATION_999_ERROR_MESSAGE = "A new Instance was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private final IdStorageService idStorageService;
  private final OrderHelperService orderHelperService;
  private final Context context;
  private final MappingMetadataDto mappingMetadata;

  public InstanceIngressCreateEventHandler(Context context,
                                           MappingMetadataDto mappingMetadata,
                                           IdStorageService idStorageService,
                                           OrderHelperService orderHelperService) {
    this.context = context;
    this.mappingMetadata = mappingMetadata;
    this.orderHelperService = orderHelperService;
    this.idStorageService = idStorageService;
  }

  @Override
  public CompletableFuture<InstanceIngressPayload> handle(InstanceIngressPayload payload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (payload == null || isEmpty(payload.getSourceRecordObject())
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      Record targetRecord = Json.decodeValue(payload.getSourceRecordObject(), Record.class);
      var sourceContent = targetRecord.getParsedRecord().getContent().toString();

      if (AdditionalFieldsUtil.getValue(targetRecord, TAG_999, SUBFIELD_I).isPresent()) {
        LOGGER.error(INSTANCE_CREATION_999_ERROR_MESSAGE);
        return CompletableFuture.failedFuture(new EventProcessingException(INSTANCE_CREATION_999_ERROR_MESSAGE));
      }


      LOGGER.info("Create instance with recordId: {}", targetRecord.getId());

      Future<RecordToEntity> recordToInstanceFuture = idStorageService.store(targetRecord.getId(), getInstanceId(targetRecord), context.getTenantId());
      recordToInstanceFuture.onSuccess(res -> {
          String instanceId = res.getEntityId();
          mappingMetadata.get(jobExecutionId, context)
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
              JsonObject instanceAsJson = prepareInstance(payload, instanceId);
              List<String> requiredFieldsErrors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
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

  private String getInstanceId(Record record) {
    String subfield999ffi = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    return isEmpty(subfield999ffi) ? UUID.randomUUID().toString() : subfield999ffi;
  }

  private JsonObject prepareInstance(InstanceIngressPayload payload) {
    JsonObject instanceAsJson = new JsonObject(payload.getSourceRecordObject());
    instanceAsJson.put(ID_KEY, payload.getSourceRecordIdentifier());
    instanceAsJson.put(SOURCE_KEY, InstanceIngressPayload.SourceType.BIBFRAME.value());

    LOGGER.debug("Creating instance with id: {}", payload.getSourceRecordIdentifier());
    return instanceAsJson;
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
}
