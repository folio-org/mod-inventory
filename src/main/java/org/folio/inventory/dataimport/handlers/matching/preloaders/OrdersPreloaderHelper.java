package org.folio.inventory.dataimport.handlers.matching.preloaders;


import static java.util.Objects.isNull;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.support.CqlHelper.buildMultipleValuesCqlQuery;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.vertx.core.json.JsonArray;

import org.folio.DataImportEventPayload;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.processing.exceptions.MatchingException;

public class OrdersPreloaderHelper {

    private static final String ORDER_LINES_CQL_PATTERN = "purchaseOrder.workflowStatus==Open AND %s";

    private OrdersClient ordersClient;

    public OrdersPreloaderHelper(OrdersClient ordersClient) {
        this.ordersClient = ordersClient;
    }

    public CompletableFuture<List<String>> preload(DataImportEventPayload eventPayload,
                                                   PreloadingFields preloadingField,
                                                   List<String> loadingParameters,
                                                   Function<JsonArray, List<String>> convertPreloadResult) {
        if (isNull(loadingParameters) || loadingParameters.isEmpty()) {
            throw new IllegalArgumentException("Loading parameters for Orders preloading must not be empty");
        }

        switch (preloadingField) {
            case POL: {
                String cqlCondition = buildMultipleValuesCqlQuery("poLineNumber==", loadingParameters);
                return getPoLineCollection(cqlCondition, eventPayload, convertPreloadResult);
            }
            case VRN: {
                String cqlCondition = buildMultipleValuesCqlQuery("vendorDetail.referenceNumbers=/@refNumber ",
                        loadingParameters);
                return getPoLineCollection(cqlCondition, eventPayload, convertPreloadResult);
            }
            default: {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown preloading field"));
            }
        }
    }

    private CompletableFuture<List<String>> getPoLineCollection(String cqlCondition,
                                                                DataImportEventPayload eventPayload,
                                                                Function<JsonArray, List<String>> convertPreloadResult) {
        Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());
        String cql = String.format(ORDER_LINES_CQL_PATTERN, cqlCondition);

        return ordersClient.getPoLineCollection(cql, context)
                .thenApply(poLines -> {
                    if (poLines.isEmpty() || poLines.get().isEmpty()) {
                        throw new MatchingException("Not found POL");
                    }
                    return convertPreloadResult.apply(poLines.get());
                });
    }
}
