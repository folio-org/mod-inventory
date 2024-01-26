package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;

import org.folio.DataImportEventPayload;
import org.folio.inventory.support.JsonArrayHelper;

public class ItemPreloader extends AbstractPreloader {

    private static final String LOCATIONS_FIELD = "locations";
    private static final String HOLDINGS_ID_FIELD = "holdingId";
    private static final String ITEM_ENTITY_NAME = "item";
    private static final String ITEM_TARGET_FIELD_NAME = "item.holdingsRecords.id";

    private OrdersPreloaderHelper ordersPreloaderHelper;

    public ItemPreloader(OrdersPreloaderHelper ordersPreloaderHelper) {
        this.ordersPreloaderHelper = ordersPreloaderHelper;
    }

    @Override
    protected String getMatchEntityName() {
        return ITEM_ENTITY_NAME;
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return ITEM_TARGET_FIELD_NAME;
    }

    @Override
    protected CompletableFuture<Optional<List<String>>> doPreloading(DataImportEventPayload eventPayload,
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
