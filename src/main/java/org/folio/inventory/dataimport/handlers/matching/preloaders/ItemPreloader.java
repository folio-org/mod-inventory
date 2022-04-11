package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.DataImportEventPayload;
import org.folio.rest.acq.model.Location;
import org.folio.rest.acq.model.PoLine;

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
        return ordersPreloaderHelper.preload(eventPayload, preloadingField, loadingParameters, this::convertPreloadResult);
    }

    private List<String> convertPreloadResult(List<PoLine> poLines) {
        return poLines.stream()
                .flatMap(poLine -> poLine.getLocations().stream().map(Location::getHoldingId))
                .collect(Collectors.toList());
    }
}
