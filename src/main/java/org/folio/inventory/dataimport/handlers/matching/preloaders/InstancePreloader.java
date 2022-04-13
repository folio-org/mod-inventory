package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.DataImportEventPayload;
import org.folio.rest.acq.model.PoLine;

public class InstancePreloader extends AbstractPreloader {

    private OrdersPreloaderHelper ordersPreloaderHelper;

    public InstancePreloader(OrdersPreloaderHelper ordersPreloaderHelper) {
        this.ordersPreloaderHelper = ordersPreloaderHelper;
    }

    @Override
    protected String getMatchEntityName() {
        return "instance";
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return "instance.id";
    }

    @Override
    protected CompletableFuture<List<String>> doPreloading(DataImportEventPayload eventPayload,
                                                           PreloadingFields preloadingField,
                                                           List<String> loadingParameters) {
        return ordersPreloaderHelper.preload(eventPayload, preloadingField, loadingParameters, this::extractInstanceIdsForInstances);
    }

    private List<String> extractInstanceIdsForInstances(List<PoLine> poLines) {
        return poLines.stream()
                .map(PoLine::getInstanceId)
                .distinct()
                .collect(Collectors.toList());
    }
}
