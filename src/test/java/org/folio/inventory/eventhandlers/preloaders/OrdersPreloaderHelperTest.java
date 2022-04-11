package org.folio.inventory.eventhandlers.preloaders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.DataImportEventPayload;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.dataimport.handlers.matching.preloaders.OrdersPreloaderHelper;
import org.folio.inventory.dataimport.handlers.matching.preloaders.PreloadingFields;
import org.folio.rest.acq.model.PoLine;
import org.folio.rest.acq.model.PoLineCollection;

@RunWith(MockitoJUnitRunner.class)
public class OrdersPreloaderHelperTest {

    private static final String NOT_FOUND_POL_MESSAGE = "Not found POL";
    private static final String EMPTY_ORDER_PRELOADING_PARAMETERS_MESSAGE = "Loadind parameters for Orders preloading must not be empty";

    @Mock
    private OrdersClient ordersClient;
    @InjectMocks
    private final OrdersPreloaderHelper ordersPreloaderHelper = new OrdersPreloaderHelper(ordersClient);

    @Test
    @SneakyThrows
    public void shouldPreloadByPOL() {
        List<String> instanceIdsMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        PoLineCollection poLineCollectionMock = new PoLineCollection();
        poLineCollectionMock.setPoLines(instanceIdsMock.stream()
                .map(instanceId -> {
                    PoLine poLine = new PoLine();
                    poLine.setInstanceId(instanceId);
                    return poLine;})
                .collect(Collectors.toList()));

        List<String> poLineNumbersMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String orderLinesCql = String.format("purchaseOrder.workflowStatus==Open AND poLineNumber==(%s OR %s)",
                poLineNumbersMock.get(0),
                poLineNumbersMock.get(1));

        when(ordersClient.getPoLineCollection(eq(orderLinesCql), any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(poLineCollectionMock)));

        List<String> instanceIds = ordersPreloaderHelper.preload(createEventPayload(),
                        PreloadingFields.POL,
                        poLineNumbersMock,
                        getPreloadResultConverter())
                .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(instanceIds).isEqualTo(instanceIdsMock);
    }

    @Test
    @SneakyThrows
    public void shouldFailOnEmptyLoadingParameters() {
        Assertions.assertThatThrownBy(() -> ordersPreloaderHelper.preload(createEventPayload(),
                                PreloadingFields.POL,
                                Collections.emptyList(),
                                getPreloadResultConverter())
                        .get(20, TimeUnit.SECONDS))
                .hasMessage(EMPTY_ORDER_PRELOADING_PARAMETERS_MESSAGE);
    }

    @Test
    @SneakyThrows
    public void shouldFailOnEmptyResponseFromOrdersClient() {
        when(ordersClient.getPoLineCollection(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        Assertions.assertThatThrownBy(() -> ordersPreloaderHelper.preload(createEventPayload(),
                                PreloadingFields.POL,
                                Collections.singletonList(UUID.randomUUID().toString()),
                                getPreloadResultConverter())
                        .get(20, TimeUnit.SECONDS))
                .getCause()
                .hasMessage(NOT_FOUND_POL_MESSAGE);
    }

    private Function<List<PoLine>, List<String>> getPreloadResultConverter() {
        return poLines -> poLines.stream()
                .map(PoLine::getInstanceId)
                .collect(Collectors.toList());
    }

    private DataImportEventPayload createEventPayload() {
        return new DataImportEventPayload()
                .withOkapiUrl("http://localhost:9493")
                .withTenant("diku")
                .withToken("token");
    }
}
