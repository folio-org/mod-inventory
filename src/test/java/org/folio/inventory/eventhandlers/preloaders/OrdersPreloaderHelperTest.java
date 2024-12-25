package org.folio.inventory.eventhandlers.preloaders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
import org.folio.inventory.support.JsonArrayHelper;

@RunWith(MockitoJUnitRunner.class)
public class OrdersPreloaderHelperTest {

    private static final String NOT_FOUND_POL_MESSAGE = "Not found POL";
    private static final String EMPTY_ORDER_PRELOADING_PARAMETERS_MESSAGE = "Loading parameters for Orders preloading must not be empty";

    private static final String INSTANCE_ID_FIELD = "instanceId";

    @Mock
    private OrdersClient ordersClient;
    @InjectMocks
    private final OrdersPreloaderHelper ordersPreloaderHelper = new OrdersPreloaderHelper(ordersClient);

    @Test
    @SneakyThrows
    public void shouldPreloadByPOL() {
        List<String> instanceIdsMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        JsonArray poLineCollectionMock = new JsonArray(instanceIdsMock.stream()
                .map(instanceId -> new JsonObject(Map.of(INSTANCE_ID_FIELD, instanceId)))
                .collect(Collectors.toList()));

        List<String> poLineNumbersMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String orderLinesCql = String.format("poLineNumber==(%s or %s)",
                poLineNumbersMock.get(0),
                poLineNumbersMock.get(1));

        when(ordersClient.getPoLineCollection(eq(orderLinesCql), any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(poLineCollectionMock)));

        Optional<List<String>> instanceIds = ordersPreloaderHelper.preload(createEventPayload(),
                        PreloadingFields.POL,
                        poLineNumbersMock,
                        getPreloadResultConverter())
                .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(instanceIds).isPresent();
        Assertions.assertThat(instanceIds).contains(instanceIdsMock);
    }

    @Test
    @SneakyThrows
    public void shouldPreloadByVRN() {
        List<String> instanceIdsMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        JsonArray poLineCollectionMock = new JsonArray(instanceIdsMock.stream()
                .map(instanceId -> new JsonObject(Map.of(INSTANCE_ID_FIELD, instanceId)))
                .collect(Collectors.toList()));

        List<String> vendorReferenceNumbersMock = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        String orderLinesCql = String.format(
                "vendorDetail.referenceNumbers=/@refNumber (%s or %s)",
                vendorReferenceNumbersMock.get(0),
                vendorReferenceNumbersMock.get(1));

        when(ordersClient.getPoLineCollection(eq(orderLinesCql), any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(poLineCollectionMock)));

        Optional<List<String>> instanceIds = ordersPreloaderHelper.preload(createEventPayload(),
                        PreloadingFields.VRN,
                        vendorReferenceNumbersMock,
                        getPreloadResultConverter())
                .get(20, TimeUnit.SECONDS);

      Assertions.assertThat(instanceIds).isPresent();
      Assertions.assertThat(instanceIds).contains(instanceIdsMock);
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
    public void shouldReturnEmptyOptionalOnEmptyResponseFromOrdersClient() {
        when(ordersClient.getPoLineCollection(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        Optional<List<String>> instanceIds = ordersPreloaderHelper.preload(createEventPayload(),
            PreloadingFields.POL,
            Collections.singletonList(UUID.randomUUID().toString()),
            getPreloadResultConverter())
          .get(20, TimeUnit.SECONDS);

        Assertions.assertThat(instanceIds).isNotPresent();
    }

    private Function<JsonArray, List<String>> getPreloadResultConverter() {
        return poLines -> JsonArrayHelper.toList(poLines).stream()
                .map(poLine -> poLine.getString(INSTANCE_ID_FIELD))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    private DataImportEventPayload createEventPayload() {
      HashMap<String, String> context = new HashMap<>();
      context.put("userId", "testUser");
        return new DataImportEventPayload()
                .withOkapiUrl("http://localhost:9493")
                .withTenant("diku")
                .withToken("token")
          .withContext(context);
    }
}
