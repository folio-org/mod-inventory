package org.folio.inventory.dataimport.handlers.matching.preloaders;


import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.folio.DataImportEventPayload;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.processing.exceptions.MatchingException;
import org.folio.rest.acq.model.PoLine;

public class OrdersPreloaderHelper {

    private final OrdersClient ordersClient;

    public OrdersPreloaderHelper(OrdersClient ordersClient) {
        this.ordersClient = ordersClient;
    }

    public CompletableFuture<List<String>> preload(DataImportEventPayload eventPayload,
                                                   PreloadingFields preloadingField,
                                                   List<String> loadingParameters,
                                                   Function<List<PoLine>, List<String>> convertPreloadResult) {
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
                            return convertPreloadResult.apply(poLineCollection.get().getPoLines());
                        });
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
}
