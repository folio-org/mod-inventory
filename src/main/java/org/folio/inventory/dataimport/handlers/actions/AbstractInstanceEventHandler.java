package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.codehaus.plexus.util.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.HttpStatus;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageRecordsClient;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

public abstract class AbstractInstanceEventHandler implements EventHandler {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractInstanceEventHandler.class);
  protected static final String MARC_FORMAT = "MARC";
  protected static final String MARC_BIB_RECORD_FORMAT = "MARC_BIB";
  protected static final String INSTANCE_PATH = "instance";
  protected static final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");
  private static final String ID_FIELD = "id";
  private static final boolean IS_HRID_FILLING_NEEDED_FOR_INSTANCE = true;

  protected final Storage storage;
  @Getter
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  @Getter
  private final MappingMetadataCache mappingMetadataCache;
  @Getter
  private final HttpClient httpClient;

  protected AbstractInstanceEventHandler(Storage storage,
                                      PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                      MappingMetadataCache mappingMetadataCache, HttpClient httpClient) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    this.httpClient = httpClient;
  }

  protected void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getContext().put("CURRENT_EVENT_TYPE", dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put("CURRENT_NODE", Json.encode(dataImportEventPayload.getCurrentNode()));

    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(INSTANCE.value(), new JsonObject().encode());
  }

  protected org.folio.Instance defaultMapRecordToInstance(DataImportEventPayload dataImportEventPayload,
                                                          JsonObject mappingRules, MappingParameters mappingParameters) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      JsonObject parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_BIBLIOGRAPHIC.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
      var instance = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(instance))));
      return instance;
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Instance", e);
      throw new JsonMappingException("Error in default mapper.", e);
    }
  }

  protected Future<Instance> saveRecordInSrsAndHandleResponse(DataImportEventPayload payload, Record record,
                                                            Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(payload).postSourceStorageRecords(record)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(),
            Json.encode(encodeParsedRecordContent(result.bodyAsJson(Record.class))));
          LOGGER.info("Created MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}, jobExecutionId: {}",
            record.getId(), instance.getId(), payload.getTenant(), payload.getJobExecutionId());
          promise.complete(instance);
        } else {
          String msg = format("Failed to create MARC record in SRS, instanceId: '%s', jobExecutionId: '%s', status code: %s, Record: %s",
            instance.getId(), payload.getJobExecutionId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          deleteInstance(instance.getId(), payload.getJobExecutionId(), instanceCollection);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  protected Future<Instance> putRecordInSrsAndHandleResponse(DataImportEventPayload payload, Record record,
                                                             Instance instance, String matchedId) {
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(payload).putSourceStorageRecordsGenerationById(matchedId ,record)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(),
            Json.encode(encodeParsedRecordContent(result.bodyAsJson(Record.class))));
          LOGGER.info("Update MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}, jobExecutionId: {}",
            record.getId(), instance.getId(), payload.getTenant(), payload.getJobExecutionId());
          promise.complete(instance);
        } else {
          String msg = format("Failed to update MARC record in SRS, instanceId: '%s', jobExecutionId: '%s', status code: %s, Record: %s",
            instance.getId(), payload.getJobExecutionId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  protected Future<Instance> executeFieldsManipulation(Instance instance, Record record) {
    AdditionalFieldsUtil.fill001FieldInMarcRecord(record, instance.getHrid());
    if (StringUtils.isBlank(record.getMatchedId())) {
      record.setMatchedId(record.getId());
    }
    setExternalIds(record, instance);
    return AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', instance.getId())
      ? Future.succeededFuture(instance)
      : Future.failedFuture(format("Failed to add instance id '%s' to record with id '%s'", instance.getId(), record.getId()));
  }

  /**
   * Adds specified externalId and externalHrid to record and additional custom field with externalId to parsed record.
   *
   * @param record   record to update
   * @param instance externalEntity in Json
   */
  protected void setExternalIds(Record record, Instance instance) {
    if (record.getExternalIdsHolder() == null) {
      record.setExternalIdsHolder(new ExternalIdsHolder());
    }
    String externalId = record.getExternalIdsHolder().getInstanceId();
    String externalHrid = record.getExternalIdsHolder().getInstanceHrid();
    if (isNotEmpty(externalId) || isNotEmpty(externalHrid)) {
      if (AdditionalFieldsUtil.isFieldsFillingNeeded(record, instance)) {
        executeHrIdManipulation(record, instance.getJsonForStorage());
      }
    } else {
      executeHrIdManipulation(record, instance.getJsonForStorage());
    }
  }

  private void deleteInstance(String id, String jobExecutionId, InstanceCollection instanceCollection) {
    Promise<Void> promise = Promise.promise();
    instanceCollection.delete(id, success -> {
        LOGGER.info("deleteInstance:: Instance was deleted by id: '{}', jobExecutionId: '{}'", id, jobExecutionId);
        promise.complete(success.getResult());
      },
      failure -> {
        LOGGER.warn("deleteInstance:: Error deleting Instance by id: '{}', jobExecutionId: '{}', cause: {}, status code: {}",
          id, jobExecutionId, failure.getReason(), failure.getStatusCode());
        promise.fail(failure.getReason());
      });
    promise.future();
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(DataImportEventPayload payload) {
    return new SourceStorageRecordsClient(payload.getOkapiUrl(), payload.getTenant(),
      payload.getToken(), getHttpClient());
  }

  private Record encodeParsedRecordContent(Record record) {
    ParsedRecord parsedRecord = record.getParsedRecord();
    if (parsedRecord != null) {
      parsedRecord.setContent(Json.encode(parsedRecord.getContent()));
      return record.withParsedRecord(parsedRecord);
    }
    return record;
  }

  protected void setSuppressFormDiscovery(Record record, boolean suppressFromDiscovery) {
    AdditionalInfo info = record.getAdditionalInfo();
    if (info != null) {
      info.setSuppressDiscovery(suppressFromDiscovery);
    } else {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(suppressFromDiscovery));
    }
  }

  private void executeHrIdManipulation(Record record, JsonObject externalEntity) {
    var externalId = externalEntity.getString(ID_FIELD);
    var externalHrId = extractHridForInstance(externalEntity);
    var externalIdsHolder = record.getExternalIdsHolder();
    setExternalIdsForInstance(externalIdsHolder, externalId, externalHrId);
    boolean isAddedField = AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', externalId);
    if (IS_HRID_FILLING_NEEDED_FOR_INSTANCE) {
      AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(Pair.of(record, externalEntity));
    }
    if (!isAddedField) {
      throw new EventProcessingException(
        format("Failed to add externalEntity id '%s' to record with id '%s'", externalId, record.getId()));
    }
  }

  private String extractHridForInstance(JsonObject externalEntity) {
    return externalEntity.getString("hrid");
  }

  private void setExternalIdsForInstance(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setInstanceId(externalId);
    externalIdsHolder.setInstanceHrid(externalHrid);
  }
}
