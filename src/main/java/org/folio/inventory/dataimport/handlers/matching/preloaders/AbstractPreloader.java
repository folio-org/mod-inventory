package org.folio.inventory.dataimport.handlers.matching.preloaders;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.extractMatchProfile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Preloader intended to run some logic to modify existing loading query before passing it to Loader
 * It check whether match expression contains any of defined fields that imply preloading
 * If field is not found - initial query returned
 * If field is found - extract field values, pass them to preloading implementation, then -
 * construct query with new values returned from preloading implementation
 * */
public abstract class AbstractPreloader {

    private static final Logger LOG = LogManager.getLogger(AbstractPreloader.class);

    public CompletableFuture<LoadQuery> preload(LoadQuery query, DataImportEventPayload dataImportEventPayload) {
        String jobExecutionId = dataImportEventPayload.getJobExecutionId();
        String recordId = dataImportEventPayload.getContext() != null ? dataImportEventPayload.getContext().get("recordId") : null;

        if (query == null) {
            LOG.info("preload:: Received null query - JobExecutionId: {}, RecordId: {}", jobExecutionId, recordId);
            return CompletableFuture.completedFuture(null);
        }

        LOG.info("preload:: Starting preload - JobExecutionId: {}, RecordId: {}, InitialCQL: '{}'",
            jobExecutionId, recordId, query.getCql());

        MatchProfile matchProfile = extractMatchProfile(dataImportEventPayload);
        MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);
        MatchExpression matchExpression = matchDetail.getExistingMatchExpression();

        Optional<PreloadingFields> preloadingField = getPreloadingField(matchExpression);
      if (preloadingField.isEmpty()) {
        MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
        Value<?> value = reader.read(dataImportEventPayload, matchDetail);
        LoadQuery loadQuery = LoadQueryBuilder.build(value, matchDetail);
        Json.encode(value);
          LOG.info("preload:: values: {} matchDetail: {} - JobExecutionId: {}, RecordId: {}",
            Json.encode(value), Json.encode(matchDetail), jobExecutionId, recordId);
        LOG.info("preload:: Built query without preloading - JobExecutionId: {}, RecordId: {}, CQL: '{}'",
            jobExecutionId, recordId, loadQuery != null ? loadQuery.getCql() : "null");
        return CompletableFuture.completedFuture(query);
      }

        LOG.info("preload:: Found preloading field: {} - JobExecutionId: {}, RecordId: {}",
            preloadingField.get().name(), jobExecutionId, recordId);

        List<String> preloadValues = extractPreloadValues(dataImportEventPayload, matchProfile, matchDetail);
        LOG.info("preload:: Extracted preload values: {} - JobExecutionId: {}, RecordId: {}",
            preloadValues, jobExecutionId, recordId);

        return doPreloading(dataImportEventPayload, preloadingField.get(), preloadValues)
                .thenApply(values -> {
                    if (values.isEmpty()) {
                      LOG.info("preload:: No values returned from preloading, returning null - JobExecutionId: {}, RecordId: {}",
                          jobExecutionId, recordId);
                      return null;
                    }
                    LOG.info("preload:: Preloading returned values: {} - JobExecutionId: {}, RecordId: {}",
                        values.get(), jobExecutionId, recordId);

                    matchExpression.setFields(Collections.singletonList(
                            new Field().withLabel("field").withValue(getLoaderTargetFieldName())));
                    matchDetail.setExistingMatchExpression(matchExpression);

                    LoadQuery newQuery = LoadQueryBuilder.build(ListValue.of(values.get()), matchDetail);
                    LOG.info("preload:: Built new query after preloading - JobExecutionId: {}, RecordId: {}, NewCQL: '{}'",
                        jobExecutionId, recordId, newQuery != null ? newQuery.getCql() : "null");
                    return newQuery;
                });
    }

    /**
     * Reads incoming record match values from event payload
     * @return record field values according to match details
     * */
    private List<String> extractPreloadValues(DataImportEventPayload dataImportEventPayload,
                                              MatchProfile matchProfile,
                                              MatchDetail matchDetail) {
        MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
        Value value = reader.read(dataImportEventPayload, matchDetail);

        if (value.getType().equals(Value.ValueType.STRING)) {
            return Collections.singletonList((String) value.getValue());
        }

        return  ((ListValue) value).getValue();
    }

    private Optional<PreloadingFields> getPreloadingField(MatchExpression matchExpression) {
        return Arrays.stream(PreloadingFields.values())
                .filter(preloadingField -> matchExpression.getFields().stream()
                        .anyMatch(field -> field.getValue().equals(getMatchEntityName() + "." + preloadingField.getExistingMatchField())))
                .findFirst();
    }

    protected abstract String getMatchEntityName();
    protected abstract String getLoaderTargetFieldName();
    protected abstract CompletableFuture<Optional<List<String>>> doPreloading(DataImportEventPayload eventPayload,
                                                                    PreloadingFields preloadingField,
                                                                    List<String> loadingParameters);
}
