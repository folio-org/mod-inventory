package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.RECORD_ID_HEADER;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

public class CreateMarcAuthorityEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateMarcAuthorityEventHandler.class);

  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t create Authority entity: context is empty or doesn`t exists";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create a Authority entity requires a mapping profile";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingParameters and mapping rules snapshots were not found by jobExecutionId '%s'.RecordId: '%s'";
  private static final String AUTHORITY_PATH = "authority";
  private static final String RECORD_ID_HEADER = "recordId";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  public CreateMarcAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_AUTHORITY_CREATED.value());

      final var payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty()
        || StringUtils.isEmpty(payloadContext.get(MARC_AUTHORITY.value()))) {
        return CompletableFuture.failedFuture(new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      var context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      prepareEvent(dataImportEventPayload);
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      mappingMetadataCache.get(jobExecutionId, context)
        .map(parametersOptional -> parametersOptional.orElseThrow(() ->
          new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId,
            dataImportEventPayload.getContext().get(RECORD_ID_HEADER)))))
        .onSuccess(mappingMetadata -> defaultMapRecordToAuthority(dataImportEventPayload, mappingMetadata))
        .compose(v -> {
          var authorityCollection = storage.getAuthorityRecordCollection(context);
          final var authorityAsJson = prepareAuthority(dataImportEventPayload);
          var authority = Json.decodeValue(authorityAsJson.encodePrettily(), Authority.class);
          return addAuthority(authority, authorityCollection);
        })
        .onSuccess(createdAuthority -> {
          LOGGER.info("Created an Authority record by jobExecutionId: '{}' and recordId: '{}'", jobExecutionId, dataImportEventPayload.getContext().get(RECORD_ID_HEADER));
          payloadContext.put(AUTHORITY.value(), Json.encodePrettily(createdAuthority));
          future.complete(dataImportEventPayload);
        })
        .onFailure(e -> {
          LOGGER.error("Failed to save new Authority by jobExecutionId: '{}' and recordId: '{}': ",jobExecutionId, dataImportEventPayload.getContext().get(RECORD_ID_HEADER), e);
          future.completeExceptionally(e);
        });
    } catch (Exception e) {
      LOGGER.error("Failed to save new Authority", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Authority> addAuthority(Authority authority, AuthorityRecordCollection authorityCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityCollection.add(authority, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error("Error posting an Authority cause {}, status code {}", failure.getReason(), failure.getStatusCode());
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private void defaultMapRecordToAuthority(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadata) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      var mappingRules = new JsonObject(mappingMetadata.getMappingRules());
      var parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_AUTHORITY.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
      RecordMapper<Authority> recordMapper = RecordMapperBuilder.buildMapper(MARC_AUTHORITY.value());
      var authority = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(AUTHORITY.value(), Json.encode(JsonObject.mapFrom(authority)));
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Authority by jobExecutionId: '{}'.Cause: {}",dataImportEventPayload.getJobExecutionId(), e);
      throw new JsonMappingException("Error in default mapper.", e);
    }
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && dataImportEventPayload.getContext().get(MARC_AUTHORITY.value()) != null
      && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      var actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return ActionProfile.Action.CREATE == actionProfile.getAction() && AUTHORITY == actionProfile.getFolioRecord();
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

  private JsonObject prepareAuthority(DataImportEventPayload dataImportEventPayload) {
    var authorityAsJson = new JsonObject(dataImportEventPayload.getContext().get(AUTHORITY.value()));
    if (authorityAsJson.getJsonObject(AUTHORITY_PATH) != null) {
      authorityAsJson = authorityAsJson.getJsonObject(AUTHORITY_PATH);
    }
    //Uncomment when MODINVSTOR-825 ready
//    authorityAsJson.put(SOURCE_KEY, MARC_FORMAT);

    return authorityAsJson;
  }


  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(AUTHORITY.value(), new JsonObject().encode());
  }
}
