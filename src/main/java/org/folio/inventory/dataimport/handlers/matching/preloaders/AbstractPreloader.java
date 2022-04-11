package org.folio.inventory.dataimport.handlers.matching.preloaders;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

public abstract class AbstractPreloader {

    public CompletableFuture<LoadQuery> preload(LoadQuery query, DataImportEventPayload dataImportEventPayload) {
        if (query == null) {
            return CompletableFuture.completedFuture(null);
        }

        MatchProfile matchProfile = (MatchProfile)dataImportEventPayload.getCurrentNode().getContent();
        MatchDetail matchDetail = new JsonObject(Json.encode(
                matchProfile.getMatchDetails().get(0)))
                .mapTo(MatchDetail.class);
        MatchExpression matchExpression = matchDetail.getExistingMatchExpression();

        Optional<PreloadingFields> preloadingField = getPreloadingField(matchExpression);
        if (preloadingField.isEmpty()) {
            return CompletableFuture.completedFuture(query);
        }

        MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
        Value value = reader.read(dataImportEventPayload, matchDetail);
        List<String> preloadValues;
        if (value.getType().equals(Value.ValueType.STRING)) {
            preloadValues = Collections.singletonList((String) value.getValue());
        } else {
            preloadValues = ((ListValue) value).getValue();
        }

        return doPreloading(dataImportEventPayload, preloadingField.get(), preloadValues)
                .thenApply(values -> {
                    matchExpression.setFields(Collections.singletonList(
                            new Field().withLabel("field").withValue(getLoaderTargetFieldName())));
                    matchDetail.setExistingMatchExpression(matchExpression);

                    return LoadQueryBuilder.build(ListValue.of(values), matchDetail);
                });
    }

    private Optional<PreloadingFields> getPreloadingField(MatchExpression matchExpression) {
        return Arrays.stream(PreloadingFields.values())
                .filter(preloadingField -> matchExpression.getFields().stream()
                        .anyMatch(field -> field.getValue().equals(getMatchEntityName() + "." + preloadingField.getExistingMatchField())))
                .findFirst();
    }

    protected abstract String getMatchEntityName();
    protected abstract String getLoaderTargetFieldName();
    protected abstract CompletableFuture<List<String>> doPreloading(DataImportEventPayload eventPayload,
                                                                    PreloadingFields preloadingField,
                                                                    List<String> loadingParameters);
}
