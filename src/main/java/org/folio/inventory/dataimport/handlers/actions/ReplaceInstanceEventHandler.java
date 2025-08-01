package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.consortium.util.MarcRecordUtil.isSubfieldExist;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.LoggerUtil.INCOMING_RECORD_ID;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_PATH;
import static org.folio.inventory.dataimport.util.MappingConstants.INSTANCE_REQUIRED_FIELDS;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.METADATA_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.LINKED_DATA;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingMetadataDto;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.inventory.client.InstanceLinkClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.dataimport.util.ValidationUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.processing.mapping.mapper.writer.marc.MarcBibRecordModifier;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Snapshot;

public class ReplaceInstanceEventHandler extends AbstractInstanceEventHandler { // NOSONAR

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC or INSTANCE data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Instance requires a mapping profile";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  static final String USER_HAS_NO_PERMISSION_MSG = "User does not have permission to update record/instance on central tenant";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String MARC_INSTANCE_SOURCE = "MARC";
  public static final String INSTANCE_ID_TYPE = "INSTANCE";
  public static final String CENTRAL_TENANT_INSTANCE_UPDATED_FLAG = "CENTRAL_TENANT_INSTANCE_UPDATED";
  public static final String CENTRAL_TENANT_ID = "CENTRAL_TENANT_ID";
  public static final String MARC_BIB_RECORD_CREATED = "MARC_BIB_RECORD_CREATED";
  static final String CENTRAL_RECORD_UPDATE_PERMISSION = "consortia.data-import.central-record-update.execute";

  private final ConsortiumService consortiumService;
  private final InstanceLinkClient instanceLinkClient;

  public ReplaceInstanceEventHandler(Storage storage,
                                     PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                     MappingMetadataCache mappingMetadataCache,
                                     HttpClient client,
                                     ConsortiumService consortiumService, InstanceLinkClient instanceLinkClient) {
    super(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, client);
    this.consortiumService = consortiumService;
    this.instanceLinkClient = instanceLinkClient;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) { // NOSONAR
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_UPDATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null
        || payloadContext.isEmpty()
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))
        || isEmpty(dataImportEventPayload.getContext().get(INSTANCE.value()))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }
      LOGGER.info("handle:: Processing ReplaceInstanceEventHandler starting with jobExecutionId: {} and incomingRecordId: {}.",
        dataImportEventPayload.getJobExecutionId(), payloadContext.get(INCOMING_RECORD_ID));

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(),
        payloadContext.get(PAYLOAD_USER_ID));
      Instance instanceToUpdate = Instance.fromJson(new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value())));

      if (instanceToUpdate.getSource() != null && instanceToUpdate.getSource().equals(LINKED_DATA.getValue())) {
        String msg = format("handle:: Failed to update Instance with id = %s. Instance with source=LINKED_DATA cannot be updated using Data Import. Please use Linked Data Editor.", instanceToUpdate.getId());
        LOGGER.warn(msg);
        return CompletableFuture.failedFuture(new DataImportException(msg));
      }

      if (isNotBlank(payloadContext.get(CENTRAL_TENANT_ID)) && isCentralTenantInstanceUpdateForbidden(payloadContext)) {
        LOGGER.warn("handle:: Failed to process instance update, reason: '{}', jobExecutionId: '{}', recordId: '{}', chunkId: '{}'",
          USER_HAS_NO_PERMISSION_MSG, dataImportEventPayload.getJobExecutionId(), payloadContext.get(RECORD_ID_HEADER), payloadContext.get(CHUNK_ID_HEADER));
        return CompletableFuture.failedFuture(new EventProcessingException(USER_HAS_NO_PERMISSION_MSG));
      }

      if (isShadowInstance(instanceToUpdate)) {
        LOGGER.info("handle:: Processing Consortium Instance jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());
        if (isCentralTenantInstanceUpdateForbidden(payloadContext)) {
          LOGGER.warn("handle:: Failed to process instance update, reason: '{}', jobExecutionId: '{}', recordId: '{}', chunkId: '{}'",
            USER_HAS_NO_PERMISSION_MSG, dataImportEventPayload.getJobExecutionId(), payloadContext.get(RECORD_ID_HEADER), payloadContext.get(CHUNK_ID_HEADER));
          return CompletableFuture.failedFuture(new EventProcessingException(USER_HAS_NO_PERMISSION_MSG));
        }

        consortiumService.getConsortiumConfiguration(context)
          .compose(consortiumConfigurationOptional -> {
            if (consortiumConfigurationOptional.isPresent()) {
              String centralTenantId = consortiumConfigurationOptional.get().getCentralTenantId();
              Context centralTenantContext = EventHandlingUtil.constructContext(centralTenantId, context.getToken(), context.getOkapiLocation(), payloadContext.get(PAYLOAD_USER_ID));
              InstanceCollection instanceCollection = storage.getInstanceCollection(centralTenantContext);
              InstanceUtil.findInstanceById(instanceToUpdate.getId(), instanceCollection)
                .onSuccess(existedCentralTenantInstance -> {
                  LOGGER.info("handle:: Processed Consortium Instance jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());
                  processInstanceUpdate(dataImportEventPayload, instanceCollection, context, existedCentralTenantInstance, future, payloadContext, centralTenantContext.getTenantId());
                  dataImportEventPayload.getContext().put(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG, "true");
                  dataImportEventPayload.getContext().put(CENTRAL_TENANT_ID, centralTenantId);
                })
                .onFailure(e -> {
                  LOGGER.warn("Error retrieving inventory Instance from central tenant", e);
                  future.completeExceptionally(e);
                });
            } else {
              LOGGER.warn("handle:: Can't retrieve centralTenantId updating Instance by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}'", dataImportEventPayload.getJobExecutionId(),
                dataImportEventPayload.getContext().get(RECORD_ID_HEADER), dataImportEventPayload.getContext().get(CHUNK_ID_HEADER));
              future.completeExceptionally(new NotFoundException("Can't retrieve centralTenantId updating Instance"));
            }
            return Future.succeededFuture();
          });
      } else {
        String targetInstanceTenantId = dataImportEventPayload.getContext().getOrDefault(CENTRAL_TENANT_ID, dataImportEventPayload.getTenant());
        Context instanceUpdateContext = EventHandlingUtil.constructContext(targetInstanceTenantId, dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(), payloadContext.get(PAYLOAD_USER_ID));
        InstanceCollection instanceCollection = storage.getInstanceCollection(instanceUpdateContext);

        InstanceUtil.findInstanceById(instanceToUpdate.getId(), instanceCollection)
          .onSuccess(existingInstance -> {
            LOGGER.info("handle:: Instance retrieved jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());
            processInstanceUpdate(dataImportEventPayload, instanceCollection, context, existingInstance, future, payloadContext, targetInstanceTenantId);
          })
          .onFailure(e -> {
            LOGGER.warn("Error retrieving inventory Instance", e);
            future.completeExceptionally(e);
          });
      }
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private boolean isShadowInstance(Instance instanceToUpdate) {
    return CONSORTIUM_FOLIO.getValue().equals(instanceToUpdate.getSource())
      || CONSORTIUM_MARC.getValue().equals(instanceToUpdate.getSource());
  }

  private boolean isCentralTenantInstanceUpdateForbidden(HashMap<String, String> payloadContext) {
    JsonArray permissions = new JsonArray(payloadContext.getOrDefault(PERMISSIONS, "[]"));
    return !permissions.contains(CENTRAL_RECORD_UPDATE_PERMISSION);
  }

  private void processInstanceUpdate(DataImportEventPayload dataImportEventPayload, InstanceCollection instanceCollection, Context context, Instance instanceToUpdate,
                                     CompletableFuture<DataImportEventPayload> future, HashMap<String, String> payloadContext, String tenantId) {
    prepareEvent(dataImportEventPayload);

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();

    String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
    String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);

    LOGGER.info("Replace instance with jobExecutionId: {} , recordId: {} , chunkId: {}", jobExecutionId, recordId, chunkId);

    getMappingMetadataCache().get(jobExecutionId, context)
      .compose(parametersOptional -> parametersOptional
        .map(mappingMetadata -> prepareAndExecuteMapping(dataImportEventPayload, mappingMetadata, instanceToUpdate, tenantId))
        .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, jobExecutionId,
          recordId, chunkId))))
      .compose(e -> {
        JsonObject instanceAsJson = prepareTargetInstance(dataImportEventPayload, instanceToUpdate);
        List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, INSTANCE_REQUIRED_FIELDS);

        if (!errors.isEmpty()) {
          String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", errors,
            jobExecutionId, recordId, chunkId);
          LOGGER.warn(msg);
          return Future.failedFuture(msg);
        }
        String marcBibAsJson = payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value());
        org.folio.rest.jaxrs.model.Record targetRecord = Json.decodeValue(marcBibAsJson, org.folio.rest.jaxrs.model.Record.class);
        Instance mappedInstance = Instance.fromJson(instanceAsJson);
        List<String> invalidUUIDsErrors = ValidationUtil.validateUUIDs(mappedInstance);
        if (!invalidUUIDsErrors.isEmpty()) {
          String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", invalidUUIDsErrors,
            jobExecutionId, recordId, chunkId);
          LOGGER.warn(msg);
          return Future.failedFuture(msg);
        }

        markInstanceAndRecordAsDeletedIfNeeded(mappedInstance, targetRecord);
        return updateInstanceAndRetryIfOlExists(mappedInstance, instanceCollection, dataImportEventPayload)
          .compose(updatedInstance -> getPrecedingSucceedingTitlesHelper().getExistingPrecedingSucceedingTitles(mappedInstance, context))
          .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
            .map(titleJson -> titleJson.getString("id"))
            .collect(Collectors.toSet()))
          .compose(titlesIds -> getPrecedingSucceedingTitlesHelper().deletePrecedingSucceedingTitles(titlesIds, context))
          .map(mappedInstance)
          .compose(instance -> {
            if (dataImportEventPayload.getContext().containsKey(CENTRAL_TENANT_ID)) {
              return copySnapshotToOtherTenant(targetRecord.getSnapshotId(), dataImportEventPayload, tenantId).map(instance);
            }
            return Future.succeededFuture(instance);
          })
          .compose(instance -> {
            if (instanceToUpdate.getSource().equals(FOLIO.getValue())) {
              LOGGER.debug("processInstanceUpdate:: processing FOLIO Instance with id: {}", instance.getId());
              executeFieldsManipulation(instance, targetRecord);
              return saveRecordInSrsAndHandleResponse(dataImportEventPayload, targetRecord, instance, instanceCollection,
                tenantId, context.getUserId());
            }
            if (instanceToUpdate.getSource().equals(MARC.getValue())) {
              LOGGER.debug("processInstanceUpdate:: processing MARC Instance with id: {}", instance.getId());
              setExternalIds(targetRecord, instance);
              setSuppressFromDiscovery(targetRecord, instance.getDiscoverySuppress());
              if (targetRecord.getMatchedId() == null) {
                LOGGER.debug("processInstanceUpdate:: Instance with id: {} has no related marc bib. Creating new record in SRS", instance.getId());
                String matchedId = UUID.randomUUID().toString();
                return saveRecordInSrsAndHandleResponse(dataImportEventPayload, targetRecord.withId(matchedId).withMatchedId(matchedId),
                  instance, instanceCollection, tenantId, context.getUserId());
              }
              LOGGER.debug("processInstanceUpdate:: Instance with id: {} has related marc bib with id: {}. Updating record in SRS", instance.getId(), targetRecord.getMatchedId());
              return putRecordInSrsAndHandleResponse(dataImportEventPayload, targetRecord, instance,
                targetRecord.getMatchedId(), tenantId, context.getUserId());
            }
            return Future.succeededFuture(instance);
          }).compose(ar -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(mappedInstance, context).map(ar))
          .map(Json::encode);
      })
      .onComplete(ar -> {
        if (ar.succeeded()) {
          prepareSucceededResultPayload(dataImportEventPayload, ar.result(), instanceToUpdate);
          future.complete(dataImportEventPayload);
        } else {
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Error updating inventory Instance by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s': %s ", jobExecutionId,
            recordId, chunkId, ar.cause());
          LOGGER.error(errMessage);
          future.completeExceptionally(ar.cause());
        }
      });
  }

  private Future<Snapshot> copySnapshotToOtherTenant(String snapshotId, DataImportEventPayload dataImportEventPayload, String tenantId) {
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(snapshotId)
      .withStatus(Snapshot.Status.COMMITTED)
      .withProcessingStartedDate(new Date());

    var context = EventHandlingUtil.constructContext(tenantId, dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(), dataImportEventPayload.getContext().get(PAYLOAD_USER_ID));
    return postSnapshotInSrsAndHandleResponse(context, snapshot);
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value();
  }


  private JsonObject prepareTargetInstance(DataImportEventPayload dataImportEventPayload, Instance instanceToUpdate) {
    JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
    if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
      instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
    }

    Set<String> precedingSucceedingIds = new HashSet<>();
    precedingSucceedingIds.addAll(instanceToUpdate.getPrecedingTitles()
      .stream()
      .filter(pr -> isNotEmpty(pr.id))
      .map(pr -> pr.id)
      .collect(Collectors.toList()));
    precedingSucceedingIds.addAll(instanceToUpdate.getSucceedingTitles()
      .stream()
      .filter(pr -> isNotEmpty(pr.id))
      .map(pr -> pr.id)
      .collect(Collectors.toList()));
    instanceAsJson.put("id", instanceToUpdate.getId());
    instanceAsJson.put(HRID_KEY, instanceToUpdate.getHrid());
    if (instanceToUpdate.getSource() != null && (!(instanceToUpdate.getSource().equals(CONSORTIUM_FOLIO.getValue())
      || instanceToUpdate.getSource().equals(CONSORTIUM_MARC.getValue())) || instanceToUpdate.getSource().equals(FOLIO.getValue()))) {
      instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
    }
    instanceAsJson.put(METADATA_KEY, instanceToUpdate.getMetadata());
    return instanceAsJson;
  }

  private Future<Void> prepareAndExecuteMapping(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadata, Instance instanceToUpdate, String tenantId) {
    JsonObject mappingRules = new JsonObject(mappingMetadata.getMappingRules());
    MappingParameters mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);

    return prepareRecordForMapping(dataImportEventPayload, mappingParameters.getMarcFieldProtectionSettings(), instanceToUpdate, mappingParameters, tenantId)
      .onSuccess(v -> {
        org.folio.Instance mapped = defaultMapRecordToInstance(dataImportEventPayload, mappingRules, mappingParameters);
        Instance mergedInstance = InstanceUtil.mergeFieldsWhichAreNotControlled(instanceToUpdate, mapped);
        dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(mergedInstance))));
        MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));
      });
  }

  private Future<Void> prepareRecordForMapping(DataImportEventPayload eventPayload,
                                               List<MarcFieldProtectionSetting> marcFieldProtectionSettings,
                                               Instance instance, MappingParameters mappingParameters, String tenantId) {
    if (MARC_INSTANCE_SOURCE.equals(instance.getSource()) || CONSORTIUM_MARC.getValue().equals(instance.getSource())) {
      SourceStorageRecordsClient client = getSourceStorageRecordsClient(eventPayload.getOkapiUrl(), eventPayload.getToken(), tenantId, null);
      return getRecordByInstanceId(client, instance.getId())
        .compose(existingRecord -> {
          var linkingRules = Optional.ofNullable(mappingParameters.getLinkingRules());
          var context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl(),
            eventPayload.getContext().get(PAYLOAD_USER_ID));
          var incomingRecord = Json.decodeValue(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

          return loadInstanceLink(existingRecord, instance.getId(), context)
            .compose(links -> updateMarcRecordContent(incomingRecord, existingRecord, marcFieldProtectionSettings, links, linkingRules.orElse(emptyList())))
            .map(linksForUpdate -> {
              if (instance.getSource().equals(MARC.getValue())) {
                incomingRecord.setMatchedId(existingRecord.getMatchedId());
                if (nonNull(existingRecord.getGeneration())) {
                  int incrementedGeneration = existingRecord.getGeneration();
                  incomingRecord.setGeneration(++incrementedGeneration);
                }
                var updatedIncomingRecord = Json.encode(incomingRecord);
                var targetRecord = Json.decodeValue(updatedIncomingRecord, org.folio.rest.jaxrs.model.Record.class);

                AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
                AdditionalFieldsUtil.normalize035(targetRecord);
                AdditionalFieldsUtil.remove035FieldWhenRecordContainsHrId(targetRecord);
                eventPayload.getContext().put(MARC_BIBLIOGRAPHIC.value(), Json.encode(targetRecord));
              } else {
                eventPayload.getContext().put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
              }
              return linksForUpdate;
            })
            .compose(linksForUpdate -> updateInstanceLinks(instance.getId(), linksForUpdate, context));
        });
    } else if (instance.getSource().equals(FOLIO.getValue())) {
      String marcBibAsJson = eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value());
      org.folio.rest.jaxrs.model.Record targetRecord = Json.decodeValue(marcBibAsJson, org.folio.rest.jaxrs.model.Record.class);

      AdditionalFieldsUtil.updateLatestTransactionDate(targetRecord, mappingParameters);
      AdditionalFieldsUtil.move001To035(targetRecord);
      AdditionalFieldsUtil.normalize035(targetRecord);
      eventPayload.getContext().put(MARC_BIBLIOGRAPHIC.value(), Json.encode(targetRecord));
    }
    return Future.succeededFuture();
  }

  private Future<Optional<InstanceLinkDtoCollection>> updateMarcRecordContent(
    Record incomingRecord, Record existingRecord, List<MarcFieldProtectionSetting> protectionSettings,
    Optional<InstanceLinkDtoCollection> links, List<LinkingRuleDto> linkingRules) {

    var promise = Promise.<Optional<InstanceLinkDtoCollection>>promise();
    try {
      if (links.isPresent()) {
        var recordModifier = new MarcBibRecordModifier();
        recordModifier.setLinks(links.get(), linkingRules);
        var updatedContent = recordModifier.updateRecord(incomingRecord, existingRecord, protectionSettings);
        incomingRecord.getParsedRecord().setContent(updatedContent);
        if (isLinksTheSame(links.get(), recordModifier.getBibAuthorityLinksKept())) {
          promise.complete(Optional.empty());
        } else {
          promise.complete(
            Optional.of(new InstanceLinkDtoCollection().withLinks(recordModifier.getBibAuthorityLinksKept())));
        }
      } else {
        var recordModifier = new MarcRecordModifier();
        var updatedContent = recordModifier.updateRecord(incomingRecord, existingRecord, protectionSettings);
        incomingRecord.getParsedRecord().setContent(updatedContent);
        promise.complete(Optional.empty());
      }
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  private boolean isLinksTheSame(InstanceLinkDtoCollection links, List<Link> bibAuthorityLinksKept) {
    if (links.getLinks().size() != bibAuthorityLinksKept.size()) {
      return false;
    }
    for (Link link : links.getLinks()) {
      if (!bibAuthorityLinksKept.contains(link)) {
        return false;
      }
    }
    return true;
  }

  private Future<Optional<InstanceLinkDtoCollection>> loadInstanceLink(Record oldRecord, String instanceId,
                                                                       Context context) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    if (isSubfieldExist(oldRecord, '9')) {
      if (isNull(instanceId) || isBlank(instanceId)) {
        instanceId = oldRecord.getExternalIdsHolder().getInstanceId();
      }
      instanceLinkClient.getLinksByInstanceId(instanceId, context)
        .whenComplete((instanceLinkDtoCollection, throwable) -> {
          if (throwable != null) {
            LOGGER.error(throwable.getMessage());
            promise.fail(throwable);
          } else {
            promise.complete(instanceLinkDtoCollection);
          }
        });
    } else {
      promise.complete(Optional.empty());
    }

    return promise.future();
  }

  private Future<Void> updateInstanceLinks(String instanceId, Optional<InstanceLinkDtoCollection> links,
                                           Context context) {
    Promise<Void> promise = Promise.promise();
    if (links.isPresent()) {
      instanceLinkClient.updateInstanceLinks(instanceId, links.get(), context)
        .whenComplete((v, throwable) -> {
          if (throwable != null) {
            promise.fail(throwable);
          } else {
            promise.complete();
          }
        });
    } else {
      promise.complete();
    }
    return promise.future();
  }

  protected Future<Record> getRecordByInstanceId(SourceStorageRecordsClient client, String instanceId) {
    return client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE).compose(resp -> {
      if (resp.statusCode() != 200) {
        LOGGER.warn(format("Failed to retrieve MARC record by instance id: '%s', status code: %s",
          instanceId, resp.statusCode()));
        return Future.succeededFuture(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject())));
      }
      return Future.succeededFuture(resp.bodyAsJson(Record.class));
    });
  }

  public Future<Instance> updateInstanceAndRetryIfOlExists(Instance instance, InstanceCollection instanceCollection,
                                                           DataImportEventPayload eventPayload) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          processOLError(instance, instanceCollection, eventPayload, promise, failure);
        } else {
          eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void processOLError(Instance instance, InstanceCollection instanceCollection, DataImportEventPayload eventPayload, Promise<Instance> promise, Failure failure) {
    int currentRetryNumber = eventPayload.getContext().get(CURRENT_RETRY_NUMBER) == null ? 0 : Integer.parseInt(eventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      eventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("OL error updating Instance - {}, status code {}. Retry ReplaceInstanceEventHandler handler...", failure.getReason(), failure.getStatusCode());
      getActualInstanceAndReInvokeCurrentHandler(instance, instanceCollection, promise, eventPayload);
    } else {
      eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Instance update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, eventPayload.getJobExecutionId());
      LOGGER.error(errMessage);
      promise.fail(errMessage);
    }
  }

  private void getActualInstanceAndReInvokeCurrentHandler(Instance instance, InstanceCollection instanceCollection, Promise<Instance> promise, DataImportEventPayload eventPayload) {
    instanceCollection.findById(instance.getId())
      .thenAccept(actualInstance -> {
        eventPayload.getContext().put(INSTANCE.value(), Json.encode(JsonObject.mapFrom(actualInstance)));
        eventPayload.getEventsChain().remove(eventPayload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
        eventPayload.setCurrentNode(Json.decodeValue(eventPayload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
        eventPayload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
        eventPayload.getContext().remove(CURRENT_NODE_PROPERTY);

        handle(eventPayload).whenComplete((res, e) -> {
          if (e != null) {

            promise.fail(e.getMessage());
          } else {
            promise.complete();
          }
        });
      })
      .exceptionally(e -> {
        eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
        LOGGER.error("Cannot get actual Instance by id: {}", instance.getId(), e);
        promise.fail(format("Cannot get actual Instance by id: %s, cause: %s", instance.getId(), e.getMessage()));
        return null;
      });
  }

  private void prepareSucceededResultPayload(DataImportEventPayload dataImportEventPayload, String updatedInstanceAsString, Instance instanceToUpdate) {
    if (dataImportEventPayload.getContext().containsKey(CENTRAL_TENANT_ID)) {
      dataImportEventPayload.getContext().put(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG, Boolean.TRUE.toString());
    }
    if (instanceToUpdate.getSource().equals(FOLIO.getValue())) {
      dataImportEventPayload.getContext().put(MARC_BIB_RECORD_CREATED, Boolean.TRUE.toString());
    }
    if (instanceToUpdate.getSource().equals(MARC.getValue())) {
      dataImportEventPayload.getContext().put(MARC_BIB_RECORD_CREATED, Boolean.FALSE.toString());
    }

    dataImportEventPayload.getContext().put(INSTANCE.value(), updatedInstanceAsString);
    dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
  }

}
