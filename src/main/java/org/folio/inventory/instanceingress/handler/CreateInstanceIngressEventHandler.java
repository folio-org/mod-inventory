package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_A;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_B;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_035;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.inventory.domain.instances.Instance.INSTANCE_TYPE_ID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.InstanceIngressPayload.SourceType;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Snapshot.Status.PROCESSING_IN_PROGRESS;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
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
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public class CreateInstanceIngressEventHandler extends CreateInstanceEventHandler implements InstanceIngressEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceIngressEventHandler.class);
  private static final String BIBFRAME = " (bibframe)";
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
  public CompletableFuture<InstanceIngressEvent> handle(InstanceIngressEvent event) {
    try {
      LOGGER.info("Processing InstanceIngressEvent with id '{}' for instance creation", event.getId());
      if (isNull(event.getEventPayload()) || isNull(event.getEventPayload().getSourceRecordObject())) {
        var message = format("InstanceIngressEvent message does not contain required data to create Instance for eventId: '%s'",
          event.getId());
        LOGGER.error(message);
        return CompletableFuture.failedFuture(new EventProcessingException(message));
      }
      super.getMappingMetadataCache().getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE)
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
    return postSnapshotInSrsAndHandleResponse(event.getId())
      .compose(snapshot -> {
        try {
          LOGGER.info("Constructing a Record from InstanceIngressEvent with id '{}'", event.getId());
          var marcBibRecord = new org.folio.rest.jaxrs.model.Record()
            .withId(event.getId())
            .withRecordType(MARC_BIB)
            .withSnapshotId(event.getId())
            .withRawRecord(new RawRecord()
              .withId(event.getId())
              .withContent(event.getEventPayload().getSourceRecordObject())
            )
            .withParsedRecord(new ParsedRecord()
              .withId(event.getId())
              .withContent(event.getEventPayload().getSourceRecordObject())
            );
          event.getEventPayload()
            .withAdditionalProperty(MARC_BIBLIOGRAPHIC.value(), marcBibRecord);

          LOGGER.info("Manipulating fields of a Record from InstanceIngressEvent with id '{}'", event.getId());
          var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
          AdditionalFieldsUtil.updateLatestTransactionDate(marcBibRecord, mappingParameters);
          AdditionalFieldsUtil.move001To035(marcBibRecord);
          AdditionalFieldsUtil.normalize035(marcBibRecord);
          AdditionalFieldsUtil.addFieldToMarcRecord(marcBibRecord, TAG_035, SUBFIELD_A, event.getEventPayload().getSourceRecordIdentifier() + BIBFRAME);

          LOGGER.info("Mapping a Record from InstanceIngressEvent with id '{}' into an Instance", event.getId());
          var parsedRecord = new JsonObject((String) marcBibRecord.getParsedRecord().getContent());
          RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
          var instance = recordMapper.mapRecord(parsedRecord, mappingParameters, new JsonObject(mappingMetadata.getMappingRules()));
          LOGGER.info("Mapped Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
          return super.idStorageService.store(event.getId(), instance.getId(),
              context.getTenantId())
            .map(r -> instance)
            .onFailure(e -> LOGGER.error("Error creating relationship of inventory recordId '{} and instanceId '{}'", event.getId(), instance.getId()));
        } catch (Exception e) {
          LOGGER.warn("Error during preparing and executing mapping:", e);
          return Future.failedFuture(e);
        }
      });
  }

  private Future<Instance> validateInstance(org.folio.Instance instance, InstanceIngressEvent event) {
    try {
      LOGGER.info("Validating Instance from InstanceIngressEvent with id '{}':", event.getId());
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
      //q is that ok?
      LOGGER.warn("No instance type provided in mapped instance with id '{}', setting 'unspecified'", instance.getId());
      instanceAsJson.put(INSTANCE_TYPE_ID_KEY, "30fffe0e-e985-4144-b2e2-1e8179bdb41f");
    }
    return instanceAsJson;
  }

  private Future<Void> saveInstance(Instance instance, InstanceIngressEvent event) {
    LOGGER.info("Saving Instance from InstanceIngressEvent with id '{}':", event.getId());
    var targetRecord = (Record) event.getEventPayload().getAdditionalProperties().get(MARC_BIBLIOGRAPHIC.value());
    var sourceContent = targetRecord.getParsedRecord().getContent().toString();
    super.addInstance(instance, instanceCollection)
      .compose(createdInstance -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(instance, context).map(createdInstance))
      .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord, event))
      .compose(createdInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var content = reorderMarcRecordFields(sourceContent, targetContent);
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
        return saveRecordInSrsAndHandleResponse(event, targetRecord, createdInstance);
      });
    return Future.succeededFuture();
  }

  private Future<Instance> executeFieldsManipulation(Instance instance, Record srcRecord, InstanceIngressEvent event) {
    LOGGER.info("executeFieldsManipulation for an Instance with id '{}':", instance.getId());
    AdditionalFieldsUtil.fill001FieldInMarcRecord(srcRecord, instance.getHrid());
    if (StringUtils.isBlank(srcRecord.getMatchedId())) {
      srcRecord.setMatchedId(srcRecord.getId());
    }
    super.setExternalIds(srcRecord, instance);
    boolean bibframeIdSetTo999 = AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, SUBFIELD_B,
      event.getEventPayload().getSourceRecordIdentifier());
    boolean instanceIdSetTo999 = AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, SUBFIELD_I, instance.getId());
    return bibframeIdSetTo999 && instanceIdSetTo999
      ? Future.succeededFuture(instance)
      : Future.failedFuture(format("Failed to add instance id '%s' to record with id '%s'", instance.getId(), srcRecord.getId()));
  }

  protected Future<Instance> saveRecordInSrsAndHandleResponse(InstanceIngressEvent event, Record srcRecord, Instance instance) {
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
    //q is that ok?
    var snapshot = new Snapshot()
      .withJobExecutionId(id)
      .withProcessingStartedDate(new Date())
      .withStatus(PROCESSING_IN_PROGRESS);
    return super.postSnapshotInSrsAndHandleResponse(context.getOkapiLocation(), context.getToken(), snapshot, context.getTenantId());
  }

}
