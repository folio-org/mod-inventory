package org.folio.inventory.dataimport.handlers.matching.preloaders;

import java.util.List;
import java.util.stream.Collectors;

import org.folio.inventory.client.OrdersClient;
import org.folio.rest.acq.model.PoLine;

public class InstancePreloader extends AbstractPreloader {
    public InstancePreloader(OrdersClient ordersClient) {
        super(ordersClient);
    }

    @Override
    protected String getEntityName() {
        return "instance";
    }

    @Override
    protected String getLoaderTargetFieldName() {
        return "instance.id";
    }

    @Override
    protected List<String> convertPreloadResult(List<PoLine> poLines) {
        return poLines.stream()
                .map(PoLine::getInstanceId)
                .collect(Collectors.toList());
    }
}
