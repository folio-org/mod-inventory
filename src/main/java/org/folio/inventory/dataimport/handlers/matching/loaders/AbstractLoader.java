package org.folio.inventory.dataimport.handlers.matching.loaders;

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
import org.folio.rest.jaxrs.model.ReactToType;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.MAX_UUIDS_TO_DISPLAY;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.buildMultiMatchErrorMessage;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

public abstract class AbstractLoader<T> implements MatchValueLoader {

  private static final Logger LOG = LogManager.getLogger(AbstractLoader.class);

  public static final String MULTI_MATCH_IDS = "MULTI_MATCH_IDS";
  private static final String ERROR_LOAD_MSG = "Failed to load records cause: %s, status code: %s";
  private static final int MULTI_MATCH_LOAD_LIMIT = 90;
  private static final String ID_FIELD = "id";

  @Override
  public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    if (loadQuery == null) {
      return CompletableFuture.completedFuture(new LoadResult());
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(getEntityType().value());
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl(),
      eventPayload.getContext().get(PAYLOAD_USER_ID), eventPayload.getContext().get(OKAPI_REQUEST_ID));
    boolean canProcessMultiMatchResult = canProcessMultiMatchResult(eventPayload);
    PagingParameters pagingParameters = buildPagingParameters(canProcessMultiMatchResult);

    try {
      String cql = loadQuery.getCql() + addCqlSubMatchCondition(eventPayload);
      getSearchableCollection(context).findByCql(cql, pagingParameters,
        success -> {
          MultipleRecords<T> collection = success.getResult();
          if (collection.totalRecords == 1) {
            loadResult.setValue(mapEntityToJsonString(collection.records.get(0)));
          } else if (collection.totalRecords > 1) {
            if (canProcessMultiMatchResult) {
              LOG.info("Found multiple records by CQL query: [{}]. Found records IDs: {}", cql, mapEntityListToIdsJsonString(collection.records));
              loadResult.setEntityType(MULTI_MATCH_IDS);
              loadResult.setValue(mapEntityListToIdsJsonString(collection.records));
            } else {
              String idsJson = mapEntityListToIdsJsonString(collection.records);
              String errorMessage = buildMultiMatchErrorMessage(idsJson, collection.totalRecords);
              LOG.error(errorMessage);
              future.completeExceptionally(new MatchingException(errorMessage));
              return;
            }
          }
          future.complete(loadResult);
        },
        failure -> {
          LOG.error(failure.getReason());
          future.completeExceptionally(new MatchingException(format(ERROR_LOAD_MSG, failure.getReason(), failure.getStatusCode())));
        });
    } catch (Exception e) {
      LOG.error("Failed to retrieve records", e);
      future.completeExceptionally(e);
    }

    return future;
  }

  /**
   * Creates paging parameters for entities loading.
   * If matching result of current matching can be processed by next profile than returns parameters with limit = 90.
   * Otherwise, for performance needs returns paging parameters with limit = 2, which is
   * a minimum value that is necessary to get target record or identify whether multiple match result occurred.
   *
   * @param multiMatchLoadingParams - identifies whether to return paging parameters for multiple matching
   * @return {@link PagingParameters}
   */
  private PagingParameters buildPagingParameters(boolean multiMatchLoadingParams) {
    // currently, limit = 90 is used because of constraint for URL size that is used for processing multi-match result
    // in scope of https://issues.folio.org/browse/MODDICORE-251 a new approach will be introduced for multi-matching result processing
    return multiMatchLoadingParams ? new PagingParameters(MULTI_MATCH_LOAD_LIMIT, 0) : new PagingParameters(MAX_UUIDS_TO_DISPLAY, 0);
  }

  private boolean canProcessMultiMatchResult(DataImportEventPayload eventPayload) {
    List<ProfileSnapshotWrapper> childProfiles = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    return isNotEmpty(childProfiles) && ReactToType.MATCH.equals(childProfiles.get(0).getReactTo())
      && MATCH_PROFILE.equals(childProfiles.get(0).getContentType());
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return getEntityType() == entityType;
  }

  protected String getConditionByMultiMatchResult(DataImportEventPayload eventPayload) {
    return getConditionByMultipleValues(ID_FIELD, eventPayload, MULTI_MATCH_IDS);
  }

  protected String getConditionByMultipleValues(String searchField,
                                                DataImportEventPayload eventPayload,
                                                String multipleValuesKey) {
    String preparedIds = new JsonArray(eventPayload.getContext().remove(multipleValuesKey))
      .stream()
      .map(Object::toString)
      .collect(Collectors.joining(" OR "));

    return format(" AND %s == (%s)", searchField, preparedIds);
  }

  protected abstract EntityType getEntityType();

  protected abstract SearchableCollection<T> getSearchableCollection(Context context);

  protected abstract String addCqlSubMatchCondition(DataImportEventPayload eventPayload);

  protected abstract String mapEntityToJsonString(T entity);

  protected abstract String mapEntityListToIdsJsonString(List<T> entityList);
}
