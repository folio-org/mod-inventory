package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Optional.ofNullable;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_L;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_035;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_035_SUB;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Snapshot.Status.COMMITTED;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

public interface InstanceIngressEventHandler {
  String LINKED_DATA_ID = "linkedDataId";
  String INSTANCE_ID = "instanceId";
  String FAILURE = "Failed to process InstanceIngressEvent with id {}";

  CompletableFuture<Instance> handle(InstanceIngressEvent instanceIngressEvent);

  default boolean eventContainsNoData(InstanceIngressEvent event) {
    return isNull(event.getEventPayload())
      || isNull(event.getEventPayload().getSourceRecordObject())
      || isNull(event.getEventPayload().getSourceType());
  }

  default Optional<String> getInstanceId(InstanceIngressEvent event) {
    return ofNullable(event.getEventPayload())
      .map(InstanceIngressPayload::getAdditionalProperties)
      .map(ap -> ap.get(INSTANCE_ID))
      .map(o -> (String) o);
  }

  default Record constructMarcBibRecord(InstanceIngressPayload eventPayload, String recordId) {
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
    eventPayload.withAdditionalProperty(MARC_BIBLIOGRAPHIC.value(), marcBibRecord);
    return marcBibRecord;
  }

  default Future<Optional<MappingMetadataDto>> getMappingMetadata(
    Context context, Supplier<MappingMetadataCache> mappingMetadataCacheSupplier) {
    return mappingMetadataCacheSupplier.get()
      .getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
  }

  default Future<Instance> validateInstance(org.folio.Instance instance,
                                            InstanceIngressEvent event,
                                            Logger logger) {
    try {
      logger.info("Validating Instance from InstanceIngressEvent with id '{}':",
        event.getId());
      var instanceAsJson = JsonObject.mapFrom(instance);
      var errors =
        EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson,
          INSTANCE_REQUIRED_FIELDS);
      return failIfErrors(errors, event.getId(), logger)
        .orElseGet(() -> {
          var mappedInstance = Instance.fromJson(instanceAsJson);
          var uuidErrors = ValidationUtil.validateUUIDs(mappedInstance);
          return failIfErrors(uuidErrors, event.getId(), logger).orElseGet(
            () -> Future.succeededFuture(mappedInstance));
        });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  default Optional<Future<Instance>> failIfErrors(List<String> errors, String eventId, Logger logger) {
    if (errors.isEmpty()) {
      return Optional.empty();
    }
    var msg = format(
      "Mapped Instance is invalid: %s, from InstanceIngressEvent with id '%s'",
      errors, eventId);
    logger.warn(msg);
    return Optional.of(Future.failedFuture(msg));
  }

  default Future<org.folio.Instance> prepareAndExecuteMapping(MappingMetadataDto mappingMetadata, Record targetRecord,
                                                              InstanceIngressEvent event, String instanceId, Logger logger) {
    try {
      logger.info("Manipulating fields of a Record from InstanceIngressEvent with id '{}'", event.getId());
      var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
      AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
      AdditionalFieldsUtil.move001To035(targetRecord);
      AdditionalFieldsUtil.normalize035(targetRecord);
      if (event.getEventPayload().getAdditionalProperties().containsKey(LINKED_DATA_ID)) {
        AdditionalFieldsUtil.addFieldToMarcRecord(targetRecord, TAG_035, TAG_035_SUB,
          "(ld) " + event.getEventPayload().getAdditionalProperties().get(LINKED_DATA_ID));
      }

      logger.info("Mapping a Record from InstanceIngressEvent with id '{}' into an Instance", event.getId());
      var parsedRecord = new JsonObject((String) targetRecord.getParsedRecord().getContent());
      RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
      var instance = recordMapper.mapRecord(parsedRecord, mappingParameters, new JsonObject(mappingMetadata.getMappingRules()));
      instance.setId(instanceId);
      instance.setSource(event.getEventPayload().getSourceType().value());
      logger.info("Mapped Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
      return Future.succeededFuture(instance);
    } catch (Exception e) {
      logger.warn("Error during preparing and executing mapping:", e);
      return Future.failedFuture(e);
    }
  }

  default Future<Instance> executeFieldsManipulation(Instance instance, Record srcRecord, Map<String, Object> eventProperties,
                                                     BiFunction<Instance, Record, Future<Instance>> fieldsManipulationFunction) {
    if (eventProperties.containsKey(LINKED_DATA_ID)) {
      AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, SUBFIELD_L, String.valueOf(eventProperties.get(LINKED_DATA_ID)));
    }
    return fieldsManipulationFunction.apply(instance, srcRecord);
  }

  default Future<Snapshot> postSnapshotInSrsAndHandleResponse(String id, Context context, BiFunction<Context, Snapshot, Future<Snapshot>> postSnapshotFunction) {
    var snapshot = new Snapshot()
      .withJobExecutionId(id)
      .withProcessingStartedDate(new Date())
      .withStatus(COMMITTED);
    return postSnapshotFunction.apply(context, snapshot);
  }
}
