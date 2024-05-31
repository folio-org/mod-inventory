package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.codehaus.plexus.util.StringUtils.isNotEmpty;
import static org.folio.inventory.dataimport.handlers.actions.AbstractInstanceEventHandler.IS_HRID_FILLING_NEEDED_FOR_INSTANCE;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.inventory.domain.instances.Instance.ID;
import static org.folio.inventory.domain.instances.Instance.INSTANCE_TYPE_ID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.InstanceIngressPayload.SourceType;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Snapshot.Status.PROCESSING_FINISHED;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public class CreateInstanceIngressEventHandler implements InstanceIngressEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceIngressEventHandler.class);
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final MappingMetadataCache mappingMetadataCache;
  private final HttpClient httpClient;
  private final Context context;
  private final InstanceCollection instanceCollection;

  public CreateInstanceIngressEventHandler(PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                           MappingMetadataCache mappingMetadataCache,
                                           HttpClient httpClient,
                                           Context context,
                                           Storage storage) {
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.mappingMetadataCache = mappingMetadataCache;
    this.httpClient = httpClient;
    this.context = context;
    this.instanceCollection = storage.getInstanceCollection(context);
  }

  @Override
  public CompletableFuture<InstanceIngressEvent> handle(InstanceIngressEvent event) {
    try {
      LOGGER.info("Processing InstanceIngressEvent with id '{}' for instance creation", event.getId());
      if (isNull(event.getEventPayload()) || isNull(event.getEventPayload().getSourceRecordObject())) {
        var message = format("InstanceIngressEvent message does not contain required data to create Instance for eventId: '%s'",
          event.getId());
        LOGGER.error(message);
        return CompletableFuture.failedFuture(new EventProcessingException(message));
      }
      //? no IdStorageService interaction?
      mappingMetadataCache.getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE)
        .map(metadataOptional -> metadataOptional.orElseThrow(() -> new EventProcessingException("MappingMetadata was not found for marc-bib record type")))
        .compose(mappingMetadataDto -> prepareAndExecuteMapping(mappingMetadataDto, event))
        .compose(instance -> validateInstance(instance, event))
        .compose(instance -> saveInstance(instance, event));

      return CompletableFuture.completedFuture(event);
    } catch (Exception e) {
      LOGGER.error("Failed to process InstanceIngressEvent with id {}", event.getId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private Future<org.folio.Instance> prepareAndExecuteMapping(MappingMetadataDto mappingMetadata, InstanceIngressEvent event) {
    return postSnapshotInSrsAndHandleResponse()
      .compose(snapshot -> {
        try {
          LOGGER.debug("Constructing a Record from InstanceIngressEvent with id '{}'", event.getId());
          var marcBibRecord = new org.folio.rest.jaxrs.model.Record()
            .withId(event.getId()) //?
            .withRecordType(MARC_BIB)
            .withSnapshotId(snapshot.getJobExecutionId()) //?
            .withRawRecord(new RawRecord() //?
              .withId(event.getId())
              .withContent(event.getEventPayload().getSourceRecordObject())
            )
            .withParsedRecord(new ParsedRecord()
              .withId(event.getId()) //?
              .withContent(event.getEventPayload().getSourceRecordObject())
            );
          event.getEventPayload()
            .withAdditionalProperty(MARC_BIBLIOGRAPHIC.value(), marcBibRecord);

          LOGGER.debug("Manipulating fields of a Record from InstanceIngressEvent with id '{}'", event.getId());
          var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
          AdditionalFieldsUtil.updateLatestTransactionDate(marcBibRecord, mappingParameters);
          AdditionalFieldsUtil.move001To035(marcBibRecord);
          AdditionalFieldsUtil.normalize035(marcBibRecord);
          //? other manipulations how to

          LOGGER.debug("Mapping a Record from InstanceIngressEvent with id '{}' into an Instance", event.getId());
          var parsedRecord = new JsonObject((String) marcBibRecord.getParsedRecord().getContent());
          RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
          var instance = recordMapper.mapRecord(parsedRecord, mappingParameters, new JsonObject(mappingMetadata.getMappingRules()));
          LOGGER.debug("Mapped Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
          return Future.succeededFuture(instance);
        } catch (Exception e) {
          LOGGER.warn("Error during preparing and executing mapping:", e);
          return Future.failedFuture(e);
        }
      });
  }

  private Future<Instance> validateInstance(org.folio.Instance instance, InstanceIngressEvent event) {
    try {
      LOGGER.debug("Validating Instance from InstanceIngressEvent with id '{}':", event.getId());
      var instanceAsJson = prepareInstance(instance, event.getEventPayload().getSourceType());
      var errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, INSTANCE_REQUIRED_FIELDS);
      return failIfErrors(errors, event.getId())
        .orElseGet(() -> {
          var mappedInstance = Instance.fromJson(instanceAsJson);
          var uuidErrors = ValidationUtil.validateUUIDs(mappedInstance);
          return failIfErrors(uuidErrors, event.getId())
            .orElseGet(() -> Future.succeededFuture(mappedInstance));
        });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private static Optional<Future<Instance>> failIfErrors(List<String> errors, String eventId) {
    if (!errors.isEmpty()) {
      var msg = format("Mapped Instance is invalid: %s, from InstanceIngressEvent with id '%s' ", errors, eventId);
      LOGGER.warn(msg);
      return Optional.of(Future.failedFuture(msg));
    }
    return Optional.empty();
  }

  private JsonObject prepareInstance(org.folio.Instance instance, SourceType sourceType) {
    var instanceAsJson = JsonObject.mapFrom(instance);
    instanceAsJson.put(SOURCE_KEY, sourceType.value());
    if (isNull(instanceAsJson.getString(INSTANCE_TYPE_ID_KEY))) {
      instanceAsJson.put(INSTANCE_TYPE_ID_KEY, "30fffe0e-e985-4144-b2e2-1e8179bdb41f");
    }
    return instanceAsJson;
  }

  private Future<Void> saveInstance(Instance instance, InstanceIngressEvent event) {
    LOGGER.debug("Saving Instance from InstanceIngressEvent with id '{}':", event.getId());
    var targetRecord = (Record) event.getEventPayload().getAdditionalProperties().get(MARC_BIBLIOGRAPHIC.value());
    var sourceContent = targetRecord.getParsedRecord().getContent().toString();
    addInstance(instance)
      .compose(createdInstance -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(instance, context).map(createdInstance))
      .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord))
      .compose(createdInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var content = reorderMarcRecordFields(sourceContent, targetContent);
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
        return saveRecordInSrsAndHandleResponse(targetRecord, createdInstance);
      });
    return Future.succeededFuture();
  }

  private Future<Instance> addInstance(Instance instance) {
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

  private Future<Instance> executeFieldsManipulation(Instance instance, Record srcRecord) {
    LOGGER.debug("executeFieldsManipulation for an Instance with id '{}':", instance.getId());
    AdditionalFieldsUtil.fill001FieldInMarcRecord(srcRecord, instance.getHrid());
    if (StringUtils.isBlank(srcRecord.getMatchedId())) {
      srcRecord.setMatchedId(srcRecord.getId());
    }
    setExternalIds(srcRecord, instance);
    return AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, 'i', instance.getId())
      ? Future.succeededFuture(instance)
      : Future.failedFuture(format("Failed to add instance id '%s' to record with id '%s'", instance.getId(), srcRecord.getId()));
  }

  protected void setExternalIds(Record srcRecord, Instance instance) {
    if (srcRecord.getExternalIdsHolder() == null) {
      srcRecord.setExternalIdsHolder(new ExternalIdsHolder());
    }
    var externalId = srcRecord.getExternalIdsHolder().getInstanceId();
    var externalHrid = srcRecord.getExternalIdsHolder().getInstanceHrid();
    if (isNotEmpty(externalId) || isNotEmpty(externalHrid)) {
      if (AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance)) {
        executeHrIdManipulation(srcRecord, instance.getJsonForStorage());
      }
    } else {
      executeHrIdManipulation(srcRecord, instance.getJsonForStorage());
    }
  }

  private void executeHrIdManipulation(Record srcRecord, JsonObject externalEntity) {
    var externalId = externalEntity.getString(ID);
    var externalHrId = extractHridForInstance(externalEntity);
    var externalIdsHolder = srcRecord.getExternalIdsHolder();
    setExternalIdsForInstance(externalIdsHolder, externalId, externalHrId);
    boolean isAddedField = AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, 'i', externalId);
    if (IS_HRID_FILLING_NEEDED_FOR_INSTANCE) {
      AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(Pair.of(srcRecord, externalEntity));
    }
    if (!isAddedField) {
      throw new EventProcessingException(
        format("Failed to add externalEntity id '%s' to record with id '%s'", externalId, srcRecord.getId()));
    }
  }

  private String extractHridForInstance(JsonObject externalEntity) {
    return externalEntity.getString("hrid");
  }

  private void setExternalIdsForInstance(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setInstanceId(externalId);
    externalIdsHolder.setInstanceHrid(externalHrid);
  }

  protected Future<Instance> saveRecordInSrsAndHandleResponse(Record srcRecord, Instance instance) {
    LOGGER.debug("Saving record in SRS and handling a response for an Instance with id '{}':", instance.getId());
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient().postSourceStorageRecords(srcRecord)
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
          deleteInstance(instance.getId());
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient() {
    return new SourceStorageRecordsClient(context.getOkapiLocation(), context.getTenantId(), context.getToken(), httpClient);
  }

  private void deleteInstance(String id) {
    Promise<Void> promise = Promise.promise();
    instanceCollection.delete(id, success -> {
        LOGGER.info("deleteInstance:: Instance was deleted by id: '{}'", id);
        promise.complete(success.getResult());
      },
      failure -> {
        LOGGER.warn("deleteInstance:: Error deleting Instance by id: '{}', cause: {}, status code: {}",
          id, failure.getReason(), failure.getStatusCode());
        promise.fail(failure.getReason());
      });
    promise.future();
  }

  private Future<Snapshot> postSnapshotInSrsAndHandleResponse() {
    Promise<Snapshot> promise = Promise.promise();
    var snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(PROCESSING_FINISHED);
    getSourceStorageSnapshotsClient().postSourceStorageSnapshots(snapshot)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.info("postSnapshotInSrsAndHandleResponse:: Posted snapshot " +
            "with id: {} to tenant: {}", snapshot.getJobExecutionId(), context.getTenantId());
          promise.complete(result.bodyAsJson(Snapshot.class));
        } else {
          String msg = format("Failed to create snapshot in SRS, snapshot id: %s, tenant id: %s, status code: %s, snapshot: %s",
            snapshot.getJobExecutionId(), context.getTenantId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  public SourceStorageSnapshotsClient getSourceStorageSnapshotsClient() {
    return new SourceStorageSnapshotsClient(context.getOkapiLocation(), context.getTenantId(), context.getToken(), httpClient);
  }

}
