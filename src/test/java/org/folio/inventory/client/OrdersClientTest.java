package org.folio.inventory.client;

import static com.github.tomakehurst.wiremock.client.WireMock.get;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OrdersLoadingException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.rest.acq.model.PoLine;
import org.folio.rest.acq.model.PoLineCollection;

@RunWith(VertxUnitRunner.class)
public class OrdersClientTest {

  private static final String TENANT_ID = "diku";
  private static final String ORDER_LINES_URL = "/orders/order-lines";
  private static final String ORDER_LINES_CQL = "poLineNumber=10001-1";

  private final Vertx vertx = Vertx.vertx();
  private final OrdersClient ordersClient = new OrdersClient(WebClient.wrap(vertx.createHttpClient()));

  private final String instanceIdMock = UUID.randomUUID().toString();
  private final PoLineCollection orderLinesResponseMock = new PoLineCollection();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));


  private Context context;

  @Before
  public void setUp() {
    PoLine poLineMock = new PoLine();
    poLineMock.setInstanceId(instanceIdMock);
    orderLinesResponseMock.setPoLines(Collections.singletonList(poLineMock));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
                    .withQueryParam("query", new RegexPattern(".*"))
      .willReturn(WireMock.ok().withBody(Json.encode(orderLinesResponseMock))));

    context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServer.baseUrl());
  }

  @Test
  public void shouldReturnInstanceIdForOrderLine(TestContext context) {
    Async async = context.async();

    CompletableFuture<Optional<PoLineCollection>> optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, this.context);

    optionalFuture.whenComplete((result, throwable) -> {
      context.assertNull(throwable);
      context.assertTrue(result.isPresent());
      context.assertEquals(result.get().getPoLines().get(0).getInstanceId(), instanceIdMock);
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenGetNotFoundForOrderLine(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
            .withQueryParam("query", new RegexPattern(".*"))
            .willReturn(WireMock.notFound()));

    CompletableFuture<Optional<PoLineCollection>> optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, this.context);

    optionalFuture.whenComplete((result, throwable) -> {
      context.assertNull(throwable);
      context.assertTrue(result.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenGetServerErrorForOrderLine(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(ORDER_LINES_URL), true))
            .withQueryParam("query", new RegexPattern(".*"))
      .willReturn(WireMock.serverError()));

    CompletableFuture<Optional<PoLineCollection>> optionalFuture = ordersClient.getPoLineCollection(ORDER_LINES_CQL, this.context);

    optionalFuture.whenComplete((result, throwable) -> {
      context.assertNull(result);
      context.assertNotNull(throwable);
      context.assertTrue(throwable.getCause() instanceof OrdersLoadingException);
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedCqlIsNull(TestContext context) {
    Async async = context.async();

    CompletableFuture<Optional<PoLineCollection>> optionalFuture = ordersClient.getPoLineCollection(null, this.context);

    optionalFuture.whenComplete((result, throwable) -> {
      context.assertNull(result);
      context.assertNotNull(throwable);
      context.assertTrue(throwable instanceof OrdersLoadingException);
      async.complete();
    });
  }

}
