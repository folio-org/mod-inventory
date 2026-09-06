package org.folio.inventory.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.InstanceLinkDtoCollection;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.exceptions.InstanceLinksException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class InstanceLinkClientTest extends BaseWireMockTest {

  private static final String TENANT_ID = "diku";
  private static final String LINKS_API_PREFIX = "/links/instances/";

  private InstanceLinkClient instanceLinkClient;
  private Context context;
  private String instanceIdMock;

  @BeforeEach
  void setUp(Vertx vertx) {
    instanceLinkClient = new InstanceLinkClient(WebClient.wrap(vertx.createHttpClient()));
    context = EventHandlingUtil.constructContext(TENANT_ID, "token", WIRE_MOCK.baseUrl());
    instanceIdMock = UUID.randomUUID().toString();
  }

  @Test
  void shouldReturnInstanceLinkDtoCollectionWhenFound(VertxTestContext testContext) {
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    WIRE_MOCK.stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
      .willReturn(ok(dtoJson)));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      assertTrue(result.isPresent());
      assertEquals(dtoJson, Json.encode(result.get()));
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnEmptyOptionalWhenNotFound(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
      .willReturn(notFound()));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      assertFalse(result.isPresent());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldFailFutureWhenServerError(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
      .willReturn(serverError().withBody("Server error")));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(result);
      assertNotNull(throwable);
      assertInstanceOf(InstanceLinksException.class, throwable.getCause());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldUpdateInstanceLinksSuccessfully(VertxTestContext testContext) {
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    WIRE_MOCK.stubFor(put(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
      .withRequestBody(equalToJson(new JsonObject(dtoJson).encode()))
      .willReturn(aResponse().withStatus(204)));

    var future = instanceLinkClient.updateInstanceLinks(instanceIdMock, dto, context);

    future.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      testContext.completeNow();
    }));
  }

  @Test
  void shouldLogWarningWhenUpdateInstanceLinksFails(VertxTestContext testContext) {
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    WIRE_MOCK.stubFor(put(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
      .withRequestBody(equalToJson(new JsonObject(dtoJson).encode()))
      .willReturn(aResponse().withStatus(500).withBody("Update failed")));

    var future = instanceLinkClient.updateInstanceLinks(instanceIdMock, dto, context);

    future.whenComplete((result, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      testContext.completeNow();
    }));
  }
}
