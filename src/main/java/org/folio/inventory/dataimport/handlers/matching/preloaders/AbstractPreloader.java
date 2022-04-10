package org.folio.inventory.dataimport.handlers.matching.preloaders;


import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.acq.model.PoLine;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

public abstract class AbstractPreloader {

    private final OrdersClient ordersClient;

    protected AbstractPreloader(OrdersClient ordersClient) {
        this.ordersClient = ordersClient;
    }

    public CompletableFuture<LoadQuery> preload(LoadQuery query, DataImportEventPayload dataImportEventPayload) {
        MatchProfile matchProfile = (MatchProfile)dataImportEventPayload.getCurrentNode().getContent();
        MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);
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
                        .anyMatch(field -> field.getValue().equals(getEntityName() + "." + preloadingField)))
                .findFirst();
    }

    protected CompletableFuture<List<String>> doPreloading(DataImportEventPayload eventPayload,
                                                           PreloadingFields preloadingField, List<String> loadingParameters) {
        switch (preloadingField) {
            case POL: {
                Context context = constructContext(eventPayload.getTenant(),
                        eventPayload.getToken(),
                        eventPayload.getOkapiUrl());
                return ordersClient.getPoLineCollection(
                                String.format("purchaseOrder.workflowStatus==Open AND poLineNumber==(%s)",
                                        getCqlParametersString(loadingParameters)),
                                context)
                        .thenApply(poLineCollection -> {
                            if (poLineCollection.isEmpty() || poLineCollection.get().getPoLines().isEmpty()) {
                                throw new MatchingException("Not found POL");
                            }
                            return poLineCollection.get().getPoLines();
                        })
                        .thenApply(this::convertPreloadResult);
            }
            default: {
                return CompletableFuture.failedFuture(new IllegalStateException("Unknown preloading field"));
            }
        }
    }

    private String getCqlParametersString(List<String> parameters) {
        if (parameters.size() == 1) {
            return parameters.get(0);
        }
        return String.join(" OR ", parameters);
    }

    protected abstract String getEntityName();
    protected abstract String getLoaderTargetFieldName();
    protected abstract List<String> convertPreloadResult(List<PoLine> poLines);
}
