package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ReactTo;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

public abstract class AbstractLoader<T> implements MatchValueLoader {

  private static final Logger LOG = LogManager.getLogger(AbstractLoader.class);

  public static final String MULTI_MATCH_IDS = "MULTI_MATCH_IDS";

  private final Vertx vertx;

  public AbstractLoader(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    if (loadQuery == null) {
      return CompletableFuture.completedFuture(new LoadResult());
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(getEntityType().value());
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());

    vertx.runOnContext(v -> {
      try {
        String cql = loadQuery.getCql() + addCqlSubMatchCondition(eventPayload);
        getSearchableCollection(context).findByCql(cql, PagingParameters.defaults(),
          success -> {
            MultipleRecords<T> collection = success.getResult();
            if (collection.totalRecords == 1) {
              loadResult.setValue(mapEntityToJsonString(collection.records.get(0)));
            } else if (collection.totalRecords > 1) {
              if (canProcessMultiMatchResult(eventPayload)) {
                LOG.info("Found multiple records by CQL query: [{}]. Found records IDs: {}", cql, mapEntityListToIdsJsonString(collection.records));
                loadResult.setEntityType(MULTI_MATCH_IDS);
                loadResult.setValue(mapEntityListToIdsJsonString(collection.records));
              } else {
                String errorMessage = String.format("Found multiple records matching specified conditions. CQL query: [%s].%nFound records: %s", cql, Json.encodePrettily(collection.records));
                LOG.error(errorMessage);
                future.completeExceptionally(new MatchingException(errorMessage));
                return;
              }
            }
            future.complete(loadResult);
          },
          failure -> {
            LOG.error(failure.getReason());
            future.completeExceptionally(new MatchingException(failure.getReason()));
          });
      } catch (UnsupportedEncodingException e) {
        LOG.error("Failed to retrieve records", e);
        future.completeExceptionally(e);
      }
    });

    return future;
  }

  private boolean canProcessMultiMatchResult(DataImportEventPayload eventPayload) {
    List<ProfileSnapshotWrapper> childProfiles = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    return isNotEmpty(childProfiles) && ReactTo.MATCH.equals(childProfiles.get(0).getReactTo())
      && ContentType.MATCH_PROFILE.equals(childProfiles.get(0).getContentType());
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return getEntityType() == entityType;
  }

  protected String getConditionByMultiMatchResult(DataImportEventPayload eventPayload) {
    String preparedIds = new JsonArray(eventPayload.getContext().remove(MULTI_MATCH_IDS))
      .stream()
      .map(Object::toString)
      .collect(Collectors.joining(" OR "));

    return format(" AND id == (%s)", preparedIds);
  }

  protected abstract EntityType getEntityType();

  protected abstract SearchableCollection<T> getSearchableCollection(Context context);

  protected abstract String addCqlSubMatchCondition(DataImportEventPayload eventPayload);

  protected abstract String mapEntityToJsonString(T entity);

  protected abstract String mapEntityListToIdsJsonString(List<T> entityList);
}
