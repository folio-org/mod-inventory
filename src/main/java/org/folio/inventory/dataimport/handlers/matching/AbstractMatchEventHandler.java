package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.preloaders.PreloadingFields;
import org.folio.inventory.dataimport.handlers.matching.util.MatchingParametersRelations;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.MatchingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MatchExpression;

import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.extractMatchProfile;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

public abstract class AbstractMatchEventHandler implements EventHandler {
  private static final Logger LOGGER = LogManager.getLogger(AbstractMatchEventHandler.class);
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'";
  private static final String MATCHING_RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String FOUND_MULTIPLE_ENTITIES = "Found multiple entities during matching on localTenant: %s and centralTenant: %s";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";

  private MappingMetadataCache mappingMetadataCache;
  private ConsortiumService consortiumService;

  public AbstractMatchEventHandler(MappingMetadataCache mappingMetadataCache, ConsortiumService consortiumService) {
    this.mappingMetadataCache = mappingMetadataCache;
    this.consortiumService = consortiumService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setEventType(getNotMatchedEventType());
    Context context = constructContext(dataImportEventPayload.getTenant(),
      dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl(), dataImportEventPayload.getContext().get(PAYLOAD_USER_ID));

    mappingMetadataCache.get(dataImportEventPayload.getJobExecutionId(), context)
      .toCompletionStage()
      .thenCompose(metadataOptional -> metadataOptional
        .map(mappingMetadataDto -> doMatching(dataImportEventPayload, mappingMetadataDto, new MatchingParametersRelations(), context))
        .orElse(CompletableFuture.failedFuture(new EventProcessingException(MAPPING_METADATA_NOT_FOUND_MSG))))
      .whenComplete((matched, throwable) -> {
        if (throwable != null) {
          LOGGER.warn("handle:: Error during matching", throwable);
          if (!dataImportEventPayload.getTenant().equals(context.getTenantId())) {
            dataImportEventPayload.setTenant(context.getTenantId());
          }
          future.completeExceptionally(throwable);
        } else {
          if (Boolean.TRUE.equals(matched)) {
            dataImportEventPayload.setEventType(getMatchedEventType());
          } else {
            dataImportEventPayload.setEventType(getNotMatchedEventType());
          }
          future.complete(dataImportEventPayload);
        }
      });
    return future;
  }

  private CompletableFuture<Boolean> doMatching(DataImportEventPayload dataImportEventPayload, MappingMetadataDto mappingMetadataDto,
                                                MatchingParametersRelations matchingParametersRelations, Context context) {
    dataImportEventPayload.getContext().put(MAPPING_PARAMS, mappingMetadataDto.getMappingParams());
    dataImportEventPayload.getContext().put(MATCHING_RELATIONS,
      Json.encode(matchingParametersRelations.getMatchingRelations()));

    return MatchingManager.match(dataImportEventPayload)
      .thenCompose(matchedLocal -> {
        if (isConsortiumActionAvailable()) {
          return matchCentralTenantIfNeeded(dataImportEventPayload, matchedLocal, context, mappingMetadataDto, matchingParametersRelations);
        }
        return CompletableFuture.completedFuture(matchedLocal);
      });
  }

  private CompletableFuture<Boolean> matchCentralTenantIfNeeded(DataImportEventPayload dataImportEventPayload, boolean isMatchedLocal, Context context,
                                                                MappingMetadataDto mappingMetadataDto, MatchingParametersRelations matchingParametersRelations) {
    LOGGER.debug("matchCentralTenantIfNeeded :: dataImportEventPayload.tenant: {}, isMatchedLocal: {}", dataImportEventPayload.getTenant(), isMatchedLocal);
    return consortiumService.getConsortiumConfiguration(context)
      .toCompletionStage().toCompletableFuture()
      .thenCompose(consortiumConfiguration -> {
        if (consortiumConfiguration.isPresent() && !consortiumConfiguration.get().getCentralTenantId().equals(context.getTenantId())
          && !isMatchByPolOrVrn(dataImportEventPayload)) {
          LOGGER.debug("matchCentralTenantIfNeeded:: Start matching on central tenant with id: {}", consortiumConfiguration.get().getCentralTenantId());
          String localMatchedInstance = dataImportEventPayload.getContext().get(getEntityType().value());
          preparePayloadBeforeConsortiumProcessing(dataImportEventPayload, consortiumConfiguration.get(), mappingMetadataDto, matchingParametersRelations);
          return MatchingManager.match(dataImportEventPayload)
            .thenCompose(isMatchedConsortium -> {
              dataImportEventPayload.setTenant(context.getTenantId());
              if (isMatchedConsortium && isMatchedLocal && !isShadowEntity(localMatchedInstance, dataImportEventPayload.getContext().get(getEntityType().value()))) {
                LOGGER.warn("matchCentralTenantIfNeeded:: Found multiple results during matching on local tenant: {} and central tenant: {} ",
                  context.getTenantId(), consortiumConfiguration.get().getCentralTenantId());
                return CompletableFuture.failedFuture(new MatchingException(String.format(FOUND_MULTIPLE_ENTITIES, context.getTenantId(), consortiumConfiguration.get().getCentralTenantId())));
              }
              if (StringUtils.isEmpty(dataImportEventPayload.getContext().get(getEntityType().value()))) {
                dataImportEventPayload.getContext().put(getEntityType().value(), localMatchedInstance);
              } else {
                dataImportEventPayload.getContext().put(CENTRAL_TENANT_ID_KEY, consortiumConfiguration.get().getCentralTenantId());
                LOGGER.info("matchCentralTenantIfNeeded:: Matched on central tenant: {}", consortiumConfiguration.get().getCentralTenantId());
              }
              return CompletableFuture.completedFuture(isMatchedConsortium || isMatchedLocal);
            });
        }
        LOGGER.debug("matchCentralTenantIfNeeded:: Consortium configuration for tenant: {} not found", context.getTenantId());
        return CompletableFuture.completedFuture(isMatchedLocal);
      });
  }

  private boolean isMatchByPolOrVrn(DataImportEventPayload dataImportEventPayload) {
    MatchProfile matchProfile = extractMatchProfile(dataImportEventPayload);
    MatchExpression matchExpression = matchProfile.getMatchDetails().get(0).getExistingMatchExpression();
    return matchExpression.getFields().stream()
      .anyMatch(field -> field.getValue().endsWith("." + PreloadingFields.POL.getExistingMatchField())
        || field.getValue().endsWith("." + PreloadingFields.VRN.getExistingMatchField()));
  }

  private void preparePayloadBeforeConsortiumProcessing(DataImportEventPayload dataImportEventPayload, ConsortiumConfiguration consortiumConfiguration,
                                                        MappingMetadataDto mappingMetadataDto, MatchingParametersRelations matchingParametersRelations) {
    dataImportEventPayload.setTenant(consortiumConfiguration.getCentralTenantId());
    dataImportEventPayload.getContext().put(MAPPING_PARAMS, mappingMetadataDto.getMappingParams());
    dataImportEventPayload.getContext().put(MATCHING_RELATIONS,
      Json.encode(matchingParametersRelations.getMatchingRelations()));
    dataImportEventPayload.getContext().remove(getEntityType().value());
  }

  private boolean isShadowEntity(String localEntity, String matchedEntity) {
    if (localEntity != null && matchedEntity != null) {
      JsonObject localEntityAsJson = new JsonObject(localEntity);
      JsonObject matchedEntityAsJson = new JsonObject(matchedEntity);
      return StringUtils.equals(localEntityAsJson.getString("id"), matchedEntityAsJson.getString("id"));
    }
    return false;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getExistingRecordType() == getEntityType();
    }
    return false;
  }

  protected abstract EntityType getEntityType();

  protected abstract String getMatchedEventType();

  protected abstract String getNotMatchedEventType();

  protected abstract boolean isConsortiumActionAvailable();
}
