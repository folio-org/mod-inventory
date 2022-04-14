package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;

import org.folio.DataImportEventPayload;
import org.folio.inventory.support.JsonArrayHelper;

public class InstancePreloader extends AbstractPreloader {

    private static final String INSTANCE_ID_FIELD = "instanceId";
    private static final String INSTANCE_ENTITY_NAME = "instance";
    private static final String INSTANCE_TARGET_FIELD_NAME = "instance.id";

    private OrdersPreloaderHelper ordersPreloaderHelper;

    public InstancePreloader(OrdersPreloaderHelper ordersPreloaderHelper) {
        this.ordersPreloaderHelper = ordersPreloaderHelper;
    }

    @Override
    protected String getMatchEntityName() {
        return INSTANCE_ENTITY_NAME;
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return INSTANCE_TARGET_FIELD_NAME;
    }

    @Override
    protected CompletableFuture<List<String>> doPreloading(DataImportEventPayload eventPayload,
                                                           PreloadingFields preloadingField,
                                                           List<String> loadingParameters) {
        return ordersPreloaderHelper.preload(eventPayload, preloadingField, loadingParameters, this::extractInstanceIdsForInstances);
    }

    private List<String> extractInstanceIdsForInstances(JsonArray poLines) {
        return JsonArrayHelper.toList(poLines).stream()
                .map(poLine -> poLine.getString(INSTANCE_ID_FIELD))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }
}
