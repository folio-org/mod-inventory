package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.stream.Collectors;

import org.folio.inventory.client.OrdersClient;
import org.folio.rest.acq.model.Location;
import org.folio.rest.acq.model.PoLine;

public class ItemPreloader extends AbstractPreloader {
    public ItemPreloader(OrdersClient ordersClient) {
        super(ordersClient);
    }

    @Override
    protected String getEntityName() {
        return "item";
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return "holdingsRecords.id";
    }

    @Override
    protected List<String> convertPreloadResult(List<PoLine> poLines) {
        return poLines.stream()
                .flatMap(poLine -> poLine.getLocations().stream().map(Location::getHoldingId))
                .collect(Collectors.toList());
    }
}
