package org.folio.inventory.dataimport.handlers.matching.preloaders;

import static org.folio.inventory.dataimport.handlers.matching.preloaders.HoldingsPreloader.HOLDINGS_ID_FIELD;
import static org.folio.inventory.dataimport.handlers.matching.preloaders.HoldingsPreloader.LOCATIONS_FIELD;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;

import org.folio.DataImportEventPayload;
import org.folio.inventory.support.JsonArrayHelper;

public class ItemPreloader extends AbstractPreloader {

    private OrdersPreloaderHelper ordersPreloaderHelper;

    public ItemPreloader(OrdersPreloaderHelper ordersPreloaderHelper) {
        this.ordersPreloaderHelper = ordersPreloaderHelper;
    }

    @Override
    protected String getMatchEntityName() {
        return "item";
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return "item.holdingsRecords.id";
    }

    @Override
    protected CompletableFuture<List<String>> doPreloading(DataImportEventPayload eventPayload,
                                                           PreloadingFields preloadingField,
                                                           List<String> loadingParameters) {
        return ordersPreloaderHelper.preload(eventPayload, preloadingField, loadingParameters, this::extractHoldingsIdsForItems);
    }

    private List<String> extractHoldingsIdsForItems(JsonArray poLines) {
        return JsonArrayHelper.toList(poLines).stream()
                .flatMap(poLine -> JsonArrayHelper.toList(poLine.getJsonArray(LOCATIONS_FIELD)).stream()
                        .map(location -> location.getString(HOLDINGS_ID_FIELD)))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }
}
