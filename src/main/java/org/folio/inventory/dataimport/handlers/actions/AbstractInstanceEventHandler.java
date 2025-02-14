package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.codehaus.plexus.util.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_PATH;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;
import static org.folio.inventory.domain.instances.Instance.ID;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.HttpStatus;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.inventory.client.wrappers.SourceStorageSnapshotsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
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
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;

import static org.folio.inventory.dataimport.util.ParsedRecordUtil.LEADER_STATUS_DELETED;

public abstract class AbstractInstanceEventHandler implements EventHandler {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractInstanceEventHandler.class);
  protected static final String MARC_FORMAT = "MARC";
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

  protected Future<Instance> saveRecordInSrsAndHandleResponse(DataImportEventPayload payload, Record srcRecord,
                                                              Instance instance, InstanceCollection instanceCollection,
                                                              String tenantId, String userId) {
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(payload.getOkapiUrl(), payload.getToken(), tenantId, userId).postSourceStorageRecords(srcRecord)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(),
            Json.encode(encodeParsedRecordContent(result.bodyAsJson(Record.class))));
          LOGGER.info("Created MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}, jobExecutionId: {}",
            srcRecord.getId(), instance.getId(), payload.getTenant(), payload.getJobExecutionId());
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

  protected Future<Instance> putRecordInSrsAndHandleResponse(DataImportEventPayload payload, Record srcRecord,
                                                             Instance instance, String matchedId, String tenantId, String userId) {
    Promise<Instance> promise = Promise.promise();
    getSourceStorageRecordsClient(payload.getOkapiUrl(), payload.getToken(), tenantId, userId)
      .putSourceStorageRecordsGenerationById(matchedId ,srcRecord)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(),
            Json.encode(encodeParsedRecordContent(result.bodyAsJson(Record.class))));
          LOGGER.info("Update MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}, jobExecutionId: {}",
            srcRecord.getId(), instance.getId(), payload.getTenant(), payload.getJobExecutionId());
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

  protected Future<Snapshot> postSnapshotInSrsAndHandleResponse(Context context, Snapshot snapshot) {
    Promise<Snapshot> promise = Promise.promise();
    getSourceStorageSnapshotsClient(context.getOkapiLocation(), context.getToken(), context.getTenantId(), context.getUserId()).postSourceStorageSnapshots(snapshot)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.info("postSnapshotInSrsAndHandleResponse:: Posted snapshot with id: {} to tenant: {}", snapshot.getJobExecutionId(), context.getTenantId());
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

  protected Future<Instance> executeFieldsManipulation(Instance instance, Record srcRecord) {
    AdditionalFieldsUtil.fill001FieldInMarcRecord(srcRecord, instance.getHrid());
    if (StringUtils.isBlank(srcRecord.getMatchedId())) {
      srcRecord.setMatchedId(srcRecord.getId());
    }
    setExternalIds(srcRecord, instance);
    return AdditionalFieldsUtil.addFieldToMarcRecord(srcRecord, TAG_999, 'i', instance.getId())
      ? Future.succeededFuture(instance)
      : Future.failedFuture(format("Failed to add instance id '%s' to record with id '%s'", instance.getId(), srcRecord.getId()));
  }

  /**
   * Adds specified externalId and externalHrid to record and additional custom field with externalId to parsed record.
   *
   * @param srcRecord   record to update
   * @param instance externalEntity in Json
   */
  protected void setExternalIds(Record srcRecord, Instance instance) {
    if (srcRecord.getExternalIdsHolder() == null) {
      srcRecord.setExternalIdsHolder(new ExternalIdsHolder());
    }
    String externalId = srcRecord.getExternalIdsHolder().getInstanceId();
    String externalHrid = srcRecord.getExternalIdsHolder().getInstanceHrid();
    if (isNotEmpty(externalId) || isNotEmpty(externalHrid)) {
      if (AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance)) {
        executeHrIdManipulation(srcRecord, instance.getJsonForStorage());
      }
    } else {
      executeHrIdManipulation(srcRecord, instance.getJsonForStorage());
    }
  }

  protected void deleteInstance(String id, String jobExecutionId, InstanceCollection instanceCollection) {
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

  public SourceStorageRecordsClient getSourceStorageRecordsClient(String okapiUrl, String token, String tenantId, String userId) {
    return new SourceStorageRecordsClientWrapper(okapiUrl, tenantId, token, userId, getHttpClient());
  }

  public SourceStorageSnapshotsClient getSourceStorageSnapshotsClient(String okapiUrl, String token, String tenantId, String userId) {
    return new SourceStorageSnapshotsClientWrapper(okapiUrl, tenantId, token, userId, getHttpClient());
  }

  private Record encodeParsedRecordContent(Record srcRecord) {
    ParsedRecord parsedRecord = srcRecord.getParsedRecord();
    if (parsedRecord != null) {
      parsedRecord.setContent(Json.encode(parsedRecord.getContent()));
      return srcRecord.withParsedRecord(parsedRecord);
    }
    return srcRecord;
  }

  protected void markInstanceAndRecordAsDeletedIfNeeded(Instance instance, Record srsRecord) {
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(srsRecord.getParsedRecord());
    if (Boolean.TRUE.equals(instance.getDeleted()) || (leaderStatus.isPresent() && LEADER_STATUS_DELETED == leaderStatus.get())) {
      instance.setDeleted(true);
      instance.setDiscoverySuppress(true);
      instance.setStaffSuppress(true);

      srsRecord.withState(Record.State.DELETED);
      srsRecord.setDeleted(true);
      setSuppressFromDiscovery(srsRecord, true);
      ParsedRecordUtil.updateLeaderStatus(srsRecord.getParsedRecord(), LEADER_STATUS_DELETED);
    }
  }

  protected void setSuppressFromDiscovery(Record srcRecord, boolean suppressFromDiscovery) {
    AdditionalInfo info = srcRecord.getAdditionalInfo();
    if (info != null) {
      info.setSuppressDiscovery(suppressFromDiscovery);
    } else {
      srcRecord.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(suppressFromDiscovery));
    }
  }

  protected String getInstanceId(Record record) {
    String subfield999ffi = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    return isEmpty(subfield999ffi) ? UUID.randomUUID().toString() : subfield999ffi;
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
}
