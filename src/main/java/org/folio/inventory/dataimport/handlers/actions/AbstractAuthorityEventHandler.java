package org.folio.inventory.dataimport.handlers.actions;

import static io.vertx.core.json.JsonObject.mapFrom;
import static java.lang.String.format;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.ActionProfile;
import org.folio.ActionProfile.FolioRecord;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileType;
import org.folio.rest.jaxrs.model.Record;

public abstract class AbstractAuthorityEventHandler implements EventHandler {

  protected static final Logger LOGGER = LogManager.getLogger(AbstractAuthorityEventHandler.class);

  private static final String ACTION_SUCCEED_MSG_PATTERN = "Action '%s' for record '%s' succeed.";
  private static final String ACTION_FAILED_MSG_PATTERN = "Action '%s' for record '%s' failed.";
  private static final String MAPPING_FAILED_MSG_PATTERN = "Failed to map '%s' to '%s'.";
  private static final String META_INFO_MSG_PATTERN = "JobExecutionId: '%s', RecordId: '%s', ChunkId: '%s'";

  private static final String UNEXPECTED_PAYLOAD_MSG = "Unexpected payload";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG =
    "MappingParameters and mapping rules snapshots were not found.";

  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  public static final String AUTHORITY_EXTENDED = "AUTHORITY_EXTENDED";

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  private static boolean isAuthorityExtended = isAuthorityExtendedMode();

  protected AbstractAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    logParametersEventHandler(LOGGER, payload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (!isExpectedPayload(payload)) {
        throw new EventProcessingException(UNEXPECTED_PAYLOAD_MSG);
      }

      prepareEvent(payload);

      var context = constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl(), payload.getContext().get(EventHandlingUtil.USER_ID));
      var jobExecutionId = payload.getJobExecutionId();
      mappingMetadataCache.get(jobExecutionId, context)
        .map(mapMetadataOrFail())
        .compose(mappingMetadata -> mapAuthority(payload, mappingMetadata))
        .compose(authority -> handleProfileAction(authority, context, payload))
        .onSuccess(successHandler(payload, future))
        .onFailure(failureHandler(payload, future));
    } catch (Exception e) {
      LOGGER.error(constructMsg(format(ACTION_FAILED_MSG_PATTERN, profileAction(), targetRecordType()), payload), e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (isExpectedPayload(payload)) {
      if (profileContentType() == ACTION_PROFILE) {
        var actionProfile = mapFrom(payload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
        return isEligibleActionProfile(actionProfile);
      } else if (profileContentType() == MAPPING_PROFILE) {
        var mappingProfile = mapFrom(payload.getCurrentNode().getContent()).mapTo(MappingProfile.class);
        return isEligibleMappingProfile(mappingProfile);
      }
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  protected abstract Future<Authority> processAuthority(Authority authority, AuthorityRecordCollection authorityCollection, DataImportEventPayload payload);

  protected abstract String nextEventType();

  protected abstract ActionProfile.Action profileAction();

  protected abstract ActionProfile.FolioRecord sourceRecordType();

  protected abstract ActionProfile.FolioRecord targetRecordType();

  protected abstract ProfileType profileContentType();

  protected abstract void publishEvent(DataImportEventPayload payload);

  private boolean isEligibleMappingProfile(MappingProfile mappingProfile) {
    return mappingProfile.getExistingRecordType() == EntityType.fromValue(sourceRecordType().value());
  }

  private boolean isEligibleActionProfile(ActionProfile actionProfile) {
    return profileAction() == actionProfile.getAction() && targetRecordType() == actionProfile.getFolioRecord();
  }

  private Future<Authority> handleProfileAction(Authority authority,
                                                Context context,
                                                DataImportEventPayload payload) {
    var authorityCollection = storage.getAuthorityRecordCollection(context);
    return processAuthority(authority, authorityCollection, payload);
  }

  private Handler<Throwable> failureHandler(DataImportEventPayload payload,
                                            CompletableFuture<DataImportEventPayload> future) {
    return e -> {
      LOGGER.error(constructMsg(format(ACTION_FAILED_MSG_PATTERN, profileAction(), targetRecordType()), payload), e);
      future.completeExceptionally(e);
    };
  }

  private Handler<Authority> successHandler(DataImportEventPayload payload,
                                            CompletableFuture<DataImportEventPayload> future) {
    return authority -> {
      LOGGER.info(constructMsg(format(ACTION_SUCCEED_MSG_PATTERN, profileAction(), targetRecordType()), payload));
      payload.getContext().put(targetRecordType().value(), Json.encode(authority));
      publishEvent(payload);
      future.complete(payload);
    };
  }

  private Future<Authority> mapAuthority(DataImportEventPayload payload, MappingMetadataDto mappingMetadata) {
    try {
      var mappingRules = new JsonObject(mappingMetadata.getMappingRules());
      var mappingParameters = Json.decodeValue(mappingMetadata.getMappingParams(), MappingParameters.class);
      var parsedRecord = new JsonObject((String) new JsonObject(payload.getContext().get(sourceRecordType().value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      RecordMapper<Authority> recordMapper = isAuthorityExtended
        ? RecordMapperBuilder.buildMapper(FolioRecord.MARC_AUTHORITY_EXTENDED.value())
        : RecordMapperBuilder.buildMapper(sourceRecordType().value());
      var authority = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      authority.setSource(Authority.Source.MARC);
      return Future.succeededFuture(authority);
    } catch (Exception e) {
      var message = format(MAPPING_FAILED_MSG_PATTERN, sourceRecordType(), targetRecordType());
      LOGGER.error(constructMsg(message, payload), e);
      return Future.failedFuture(new JsonMappingException(message, e));
    }
  }

  private boolean isExpectedPayload(DataImportEventPayload payload) {
    return payload != null
      && payload.getCurrentNode() != null
      && profileContentType() == payload.getCurrentNode().getContentType()
      && payload.getContext() != null
      && !payload.getContext().isEmpty()
      && StringUtils.isNotBlank(payload.getContext().get(sourceRecordType().value()));
  }

  protected void prepareEvent(DataImportEventPayload payload) {
    payload.setEventType(nextEventType());
    payload.getEventsChain().add(payload.getEventType());
    payload.getContext().put(targetRecordType().value(), new JsonObject().encode());
  }

  protected String getChunkIdHeader(DataImportEventPayload payload) {
    return payload.getContext() == null ? "-" : payload.getContext().get(CHUNK_ID_HEADER);
  }

  protected String getRecordIdHeader(DataImportEventPayload payload) {
    return payload.getContext() == null ? "-" : payload.getContext().get(RECORD_ID_HEADER);
  }

  private Function<Optional<MappingMetadataDto>, MappingMetadataDto> mapMetadataOrFail() {
    return parameters -> parameters.orElseThrow(() -> new EventProcessingException(MAPPING_METADATA_NOT_FOUND_MSG));
  }

  protected String constructMsg(String message, DataImportEventPayload payload) {
    if (payload == null) {
      return message;
    } else {
      return message + " " + constructMetaInfoMsg(payload);
    }
  }

  private String constructMetaInfoMsg(DataImportEventPayload payload) {
    return format(
      META_INFO_MSG_PATTERN,
      payload.getJobExecutionId(),
      getRecordIdHeader(payload),
      getChunkIdHeader(payload)
    );
  }

  private static boolean isAuthorityExtendedMode() {
    return Boolean.parseBoolean(
      System.getenv().getOrDefault(AUTHORITY_EXTENDED, "false"));
  }

  /**
   * For test usage only.
   *
   * @param newIsAuthoritiesExtended New value for the env to set.
   */
  public static void setAuthorityExtendedMode(boolean newIsAuthoritiesExtended) {
    isAuthorityExtended = newIsAuthoritiesExtended;
  }
   public static boolean getIsAuthorityExtended() {
    return isAuthorityExtended;
  }
}
