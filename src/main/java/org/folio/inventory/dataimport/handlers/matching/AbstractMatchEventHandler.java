package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.util.MatchingParametersRelations;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.matching.MatchingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingMetadataDto;

import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

public abstract class AbstractMatchEventHandler implements EventHandler {

  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'";
  private static final String MATCHING_RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";

  private MappingMetadataCache mappingMetadataCache;

  public AbstractMatchEventHandler(MappingMetadataCache mappingMetadataCache) {
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    Context context = constructContext(dataImportEventPayload.getTenant(),
      dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());

    mappingMetadataCache.get(dataImportEventPayload.getJobExecutionId(), context)
      .toCompletionStage()
      .thenCompose(metadataOptional -> metadataOptional
        .map(mappingMetadataDto -> doMatching(dataImportEventPayload, mappingMetadataDto, new MatchingParametersRelations()))
        .orElse(CompletableFuture.failedFuture(new EventProcessingException(MAPPING_METADATA_NOT_FOUND_MSG))))
      .whenComplete((matched, throwable) -> {
        if (throwable != null) {
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
                                                MatchingParametersRelations matchingParametersRelations) {
    dataImportEventPayload.getContext().put(MAPPING_PARAMS, Json.encode(mappingMetadataDto));
    dataImportEventPayload.getContext().put(MATCHING_RELATIONS,
      Json.encode(matchingParametersRelations.getMatchingRelations()));

    return MatchingManager.match(dataImportEventPayload);
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

}
