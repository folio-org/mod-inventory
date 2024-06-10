package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Optional.ofNullable;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_L;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_035;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_035_SUB;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Snapshot.Status.PROCESSING_FINISHED;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public class CreateInstanceIngressEventHandler extends CreateInstanceEventHandler implements InstanceIngressEventHandler {

  private static final String LINKED_DATA_ID = "linkedDataId";
  private static final Logger LOGGER = getLogger(CreateInstanceIngressEventHandler.class);
  private static final String LD = "(ld) ";
  private static final String FAILURE = "Failed to process InstanceIngressEvent with id {}";
  private final Context context;
  private final InstanceCollection instanceCollection;

  public CreateInstanceIngressEventHandler(PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                           MappingMetadataCache mappingMetadataCache,
                                           IdStorageService idStorageService,
                                           HttpClient httpClient,
                                           Context context,
                                           Storage storage) {
    super(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, idStorageService, null, httpClient);
    this.context = context;
    this.instanceCollection = storage.getInstanceCollection(context);
  }

  @Override
  public CompletableFuture<Instance> handle(InstanceIngressEvent event) {
    try {
      LOGGER.info("Processing InstanceIngressEvent with id '{}' for instance creation", event.getId());
      var future = new CompletableFuture<Instance>();
      if (eventContainsNoData(event)) {
        var message = format("InstanceIngressEvent message does not contain required data to create Instance for eventId: '%s'", event.getId());
        LOGGER.error(message);
        return CompletableFuture.failedFuture(new EventProcessingException(message));
      }

      var targetRecord = constructMarcBibRecord(event.getEventPayload());
      var instanceId = ofNullable(event.getEventPayload().getSourceRecordIdentifier()).orElseGet(() -> getInstanceId(targetRecord));
      idStorageService.store(targetRecord.getId(), instanceId, context.getTenantId())
        .compose(res -> super.getMappingMetadataCache().getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE))
        .compose(metadataOptional -> metadataOptional.map(metadata -> prepareAndExecuteMapping(metadata, targetRecord, event, instanceId))
          .orElseGet(() -> Future.failedFuture("MappingMetadata was not found for marc-bib record type")))
        .compose(instance -> validateInstance(instance, event))
        .compose(instance -> saveInstance(instance, event))
        .onFailure(e -> {
          if (!(e instanceof DuplicateEventException)) {
            LOGGER.error(FAILURE, event.getId(), e);
          }
          future.completeExceptionally(e);
        })
        .onComplete(ar -> future.complete(ar.result()));
      return future;
    } catch (Exception e) {
      LOGGER.error(FAILURE, event.getId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private boolean eventContainsNoData(InstanceIngressEvent event) {
    return isNull(event.getEventPayload())
      || isNull(event.getEventPayload().getSourceRecordObject())
      || isNull(event.getEventPayload().getSourceType());
  }

  private Future<org.folio.Instance> prepareAndExecuteMapping(MappingMetadataDto mappingMetadata,
                                                              Record targetRecord,
                                                              InstanceIngressEvent event,
                                                              String instanceId) {
    return postSnapshotInSrsAndHandleResponse(targetRecord.getId())
      .compose(snapshot -> {
        try {
          LOGGER.info("Manipulating fields of a Record from InstanceIngressEvent with id '{}'", event.getId());
          var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
          AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
          AdditionalFieldsUtil.move001To035(targetRecord);
          AdditionalFieldsUtil.normalize035(targetRecord);
          if (event.getEventPayload().getAdditionalProperties().containsKey(LINKED_DATA_ID)) {
            AdditionalFieldsUtil.addFieldToMarcRecord(targetRecord, TAG_035, TAG_035_SUB,
              LD + event.getEventPayload().getAdditionalProperties().get(LINKED_DATA_ID));
          }

          LOGGER.info("Mapping a Record from InstanceIngressEvent with id '{}' into an Instance", event.getId());
          var parsedRecord = new JsonObject((String) targetRecord.getParsedRecord().getContent());
          RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
          var instance = recordMapper.mapRecord(parsedRecord, mappingParameters, new JsonObject(mappingMetadata.getMappingRules()));
          instance.setId(instanceId);
          instance.setSource(event.getEventPayload().getSourceType().value());
          LOGGER.info("Mapped Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
          return Future.succeededFuture(instance);
        } catch (Exception e) {
          LOGGER.warn("Error during preparing and executing mapping:", e);
          return Future.failedFuture(e);
        }
      });
  }

  private Record constructMarcBibRecord(InstanceIngressPayload eventPayload) {
    var recordId = UUID.randomUUID().toString();
    var marcBibRecord = new org.folio.rest.jaxrs.model.Record()
      .withId(recordId)
      .withRecordType(MARC_BIB)
      .withSnapshotId(recordId)
      .withRawRecord(new RawRecord()
        .withId(recordId)
        .withContent(eventPayload.getSourceRecordObject())
      )
      .withParsedRecord(new ParsedRecord()
        .withId(recordId)
        .withContent(eventPayload.getSourceRecordObject())
      );
    eventPayload
      .withAdditionalProperty(MARC_BIBLIOGRAPHIC.value(), marcBibRecord);
    return marcBibRecord;
  }

  private Future<Instance> validateInstance(org.folio.Instance instance, InstanceIngressEvent event) {
    try {
      LOGGER.info("Validating Instance from InstanceIngressEvent with id '{}':", event.getId());
      var instanceAsJson = JsonObject.mapFrom(instance);
      var errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, INSTANCE_REQUIRED_FIELDS);
      return failIfErrors(errors, event.getId())
        .orElseGet(() -> {
          var mappedInstance = Instance.fromJson(instanceAsJson);
          var uuidErrors = ValidationUtil.validateUUIDs(mappedInstance);
          return failIfErrors(uuidErrors, event.getId()).orElseGet(() -> Future.succeededFuture(mappedInstance));
        });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Optional<Future<Instance>> failIfErrors(List<String> errors, String eventId) {
    if (errors.isEmpty()) {
      return Optional.empty();
    }
    var msg = format("Mapped Instance is invalid: %s, from InstanceIngressEvent with id '%s'", errors, eventId);
    LOGGER.warn(msg);
    return Optional.of(Future.failedFuture(msg));
  }

  private Future<Instance> saveInstance(Instance instance, InstanceIngressEvent event) {
    LOGGER.info("Saving Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
    var targetRecord = (Record) event.getEventPayload().getAdditionalProperties().get(MARC_BIBLIOGRAPHIC.value());
    var sourceContent = targetRecord.getParsedRecord().getContent().toString();
    return super.addInstance(instance, instanceCollection)
      .compose(createdInstance -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(instance, context).map(createdInstance))
      .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord, event.getEventPayload().getAdditionalProperties()))
      .compose(createdInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var content = reorderMarcRecordFields(sourceContent, targetContent);
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
        return saveRecordInSrsAndHandleResponse(event, targetRecord, createdInstance);
      });
  }

  private Future<Instance> executeFieldsManipulation(Instance instance, Record srcRecord,
                                                     Map<String, Object> eventProperties) {
    if (eventProperties.containsKey(LINKED_DATA_ID)) {
      AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, SUBFIELD_L, String.valueOf(eventProperties.get(LINKED_DATA_ID)));
    }
    return super.executeFieldsManipulation(instance, srcRecord);
  }

  private Future<Instance> saveRecordInSrsAndHandleResponse(InstanceIngressEvent event, Record srcRecord, Instance instance) {
    LOGGER.info("Saving record in SRS and handling a response for an Instance with id '{}':", instance.getId());
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(context.getOkapiLocation(), context.getToken(), context.getTenantId())
      .postSourceStorageRecords(srcRecord)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.info("Created MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}",
            srcRecord.getId(), instance.getId(), context.getTenantId());
          promise.complete(instance);
        } else {
          String msg = format("Failed to create MARC record in SRS, instanceId: '%s', status code: %s, Record: %s",
            instance.getId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          super.deleteInstance(instance.getId(), event.getId(), instanceCollection);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  private Future<Snapshot> postSnapshotInSrsAndHandleResponse(String id) {
    var snapshot = new Snapshot()
      .withJobExecutionId(id)
      .withProcessingStartedDate(new Date())
      .withStatus(PROCESSING_FINISHED);
    return super.postSnapshotInSrsAndHandleResponse(context.getOkapiLocation(),
      context.getToken(), snapshot, context.getTenantId());
  }

}
