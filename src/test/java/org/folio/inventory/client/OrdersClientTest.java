package org.folio.inventory.client;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OrdersLoadingException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class OrdersClientTest extends BaseWireMockTest {

  private static final String TENANT_ID = "diku";
  private static final String ORDER_LINES_URL = "/orders/order-lines";
  private static final String ORDER_LINES_CQL = "poLineNumber=10001-1";
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String PO_LINES_FIELD = "poLines";

  private OrdersClient ordersClient;
  private String instanceIdMock;
  private Context context;

  @BeforeEach
  void setUp(Vertx vertx) {
    ordersClient = new OrdersClient(WebClient.wrap(vertx.createHttpClient()));
    instanceIdMock = UUID.randomUUID().toString();

    var poLineMock = new JsonObject(Map.of(INSTANCE_ID_FIELD, instanceIdMock));
    var orderLinesMock = new JsonObject(Map.of(PO_LINES_FIELD, new JsonArray(singletonList(poLineMock))));

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
      .withQueryParam("query", new RegexPattern(".*"))
      .willReturn(WireMock.ok().withBody(Json.encode(orderLinesMock))));

    context = EventHandlingUtil.constructContext(TENANT_ID, "token", WIRE_MOCK.baseUrl());
  }

  @Test
  void shouldReturnInstanceIdForOrderLine(VertxTestContext testContext) {
    var optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, context);

    optionalFuture.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      assertTrue(result.isPresent());
      assertEquals(result.get().getJsonObject(0).getString(INSTANCE_ID_FIELD), instanceIdMock);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundForOrderLine(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
      .withQueryParam("query", new RegexPattern(".*"))
      .willReturn(WireMock.notFound()));

    var optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, context);

    optionalFuture.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      assertTrue(result.isEmpty());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorForOrderLine(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
      .withQueryParam("query", new RegexPattern(".*"))
      .willReturn(WireMock.serverError()));

    var optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, context);

    optionalFuture.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(result);
      assertNotNull(throwable);
      assertInstanceOf(OrdersLoadingException.class, throwable.getCause());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedCqlIsNull(VertxTestContext testContext) {
    var optionalFuture = ordersClient.getPoLineCollection(null, context);

    optionalFuture.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(result);
      assertNotNull(throwable);
      assertInstanceOf(OrdersLoadingException.class, throwable);
      testContext.completeNow();
    }));
  }
}
