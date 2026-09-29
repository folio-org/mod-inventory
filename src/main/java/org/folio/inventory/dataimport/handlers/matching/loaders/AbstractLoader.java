package org.folio.inventory.dataimport.handlers.matching.loaders;

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.MAX_UUIDS_TO_DISPLAY;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.buildMultiMatchErrorMessage;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.StaticValueReaderImpl;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ReactToType;

public abstract class AbstractLoader<T> implements MatchValueLoader {

  public static final String MULTI_MATCH_IDS = "MULTI_MATCH_IDS";
  public static final String INSTANCES_IDS = "INSTANCES_IDS";
  private static final Logger LOG = LogManager.getLogger(AbstractLoader.class);
  private static final String ERROR_LOAD_MSG = "Failed to load records cause: %s, status code: %s";
  private static final int MULTI_MATCH_LOAD_LIMIT = 90;
  private static final String ID_FIELD = "id";
  private static final StaticValueReaderImpl STATIC_VALUE_READER = new StaticValueReaderImpl();

  @Override
  public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    if (loadQuery == null) {
      return CompletableFuture.completedFuture(new LoadResult());
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(getEntityType().value());
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl(),
      eventPayload.getContext().get(DataImportHeaders.USER_ID), eventPayload.getContext().get(
        XOkapiHeaders.REQUEST_ID.toLowerCase()));
    boolean canProcessMultiMatchResult = canProcessMultiMatchResult(eventPayload);
    PagingParameters pagingParameters = buildPagingParameters(canProcessMultiMatchResult);

    try {
      String cql = loadQuery.getCql() + addCqlSubMatchCondition(eventPayload)
                   + buildCombinedStaticSubMatchCondition(eventPayload);

      Consumer<Success<MultipleRecords<T>>> onSuccess = success -> {
        MultipleRecords<T> collection = success.result();
        if (collection.totalRecords() == 1) {
          loadResult.setValue(mapEntityToJsonString(collection.records().getFirst()));
        } else if (collection.totalRecords() > 1) {
          if (canProcessMultiMatchResult) {
            LOG.info("Found multiple records by CQL query: [{}]. Found records IDs: {}", cql,
              mapEntityListToIdsJsonString(collection.records()));
            loadResult.setEntityType(MULTI_MATCH_IDS);
            loadResult.setValue(mapEntityListToIdsJsonString(collection.records()));
          } else {
            String idsJson = mapEntityListToIdsJsonString(collection.records());
            String errorMessage = buildMultiMatchErrorMessage(idsJson, collection.totalRecords());
            LOG.error(errorMessage);
            future.completeExceptionally(new MatchingException(errorMessage));
            return;
          }
        }
        future.complete(loadResult);
      };
      Consumer<Failure> onFailure = failure -> {
        LOG.error(failure.reason());
        future.completeExceptionally(
          new MatchingException(format(ERROR_LOAD_MSG, failure.reason(), failure.statusCode())));
      };

      executeQuery(context, cql, pagingParameters, onSuccess, onFailure);
    } catch (Exception e) {
      LOG.error("Failed to retrieve records", e);
      future.completeExceptionally(e);
    }

    return future;
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

  /**
   * Executes the CQL query against this loader's collection. Overridable so entity-specific loaders
   * can route through a richer search call (e.g. {@link org.folio.inventory.domain.instances.InstanceCollection}'s
   * shadow-copy-aware search) without changing the shared matching flow above.
   */
  protected void executeQuery(Context context, String cql, PagingParameters pagingParameters,
                              Consumer<Success<MultipleRecords<T>>> onSuccess, Consumer<Failure> onFailure)
    throws UnsupportedEncodingException {
    getSearchableCollection(context).findByCql(cql, pagingParameters, onSuccess, onFailure);
  }

  protected abstract String addCqlSubMatchCondition(DataImportEventPayload eventPayload);

  protected abstract String mapEntityToJsonString(T entity);

  protected abstract String mapEntityListToIdsJsonString(List<T> entityList);

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
    return new PagingParameters(multiMatchLoadingParams ? MULTI_MATCH_LOAD_LIMIT : MAX_UUIDS_TO_DISPLAY, 0);
  }

  private boolean canProcessMultiMatchResult(DataImportEventPayload eventPayload) {
    List<ProfileSnapshotWrapper> childProfiles = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    return isNotEmpty(childProfiles) && ReactToType.MATCH.equals(childProfiles.getFirst().getReactTo())
           && MATCH_PROFILE.equals(childProfiles.getFirst().getContentType());
  }

  /**
   * First pass at combining a chained "submatch" into the parent query, for the common, safe case:
   * the immediate next match profile is a "Static value (submatch only)" match against the same
   * existing record type as the parent. A static value doesn't depend on the incoming record, so it
   * can be folded into the parent's own CQL and evaluated in the same query instead of relying purely
   * on a second, MULTI_MATCH_IDS-scoped round trip. This narrows the parent's result up front (avoiding
   * an intermediate ambiguous multi-match state for this pattern); the submatch step itself still runs
   * afterward unchanged.
   */
  private String buildCombinedStaticSubMatchCondition(DataImportEventPayload eventPayload) {
    List<ProfileSnapshotWrapper> childProfiles = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    if (!isNotEmpty(childProfiles) || childProfiles.size() != 1) {
      return EMPTY;
    }
    ProfileSnapshotWrapper childWrapper = childProfiles.getFirst();
    if (!MATCH_PROFILE.equals(childWrapper.getContentType()) || !ReactToType.MATCH.equals(childWrapper.getReactTo())) {
      return EMPTY;
    }

    MatchProfile childMatchProfile = extractMatchProfile(childWrapper);
    if (childMatchProfile.getExistingRecordType() != getEntityType()
        || childMatchProfile.getIncomingRecordType() != EntityType.STATIC_VALUE
        || !isNotEmpty(childMatchProfile.getMatchDetails())) {
      return EMPTY;
    }

    MatchDetail childMatchDetail = childMatchProfile.getMatchDetails().getFirst();
    Value<?> value = STATIC_VALUE_READER.read(eventPayload, childMatchDetail);
    LoadQuery childQuery = LoadQueryBuilder.build(value, childMatchDetail);
    return childQuery != null && StringUtils.isNotEmpty(childQuery.getCql())
      ? format(" AND (%s)", childQuery.getCql())
      : EMPTY;
  }

  private MatchProfile extractMatchProfile(ProfileSnapshotWrapper wrapper) {
    if (wrapper.getContent() instanceof Map map) {
      return new JsonObject(map).mapTo(MatchProfile.class);
    }
    return new JsonObject(Json.encode(wrapper.getContent())).mapTo(MatchProfile.class);
  }
}
