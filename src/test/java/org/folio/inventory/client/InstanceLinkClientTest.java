package org.folio.inventory.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.UUID;
import org.folio.InstanceLinkDtoCollection;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.InstanceLinksException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class InstanceLinkClientTest {

  private static final String TENANT_ID = "diku";
  private static final String LINKS_API_PREFIX = "/links/instances/";

  private final Vertx vertx = Vertx.vertx();
  private InstanceLinkClient instanceLinkClient;
  private Context context;
  private String instanceIdMock;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    instanceLinkClient = new InstanceLinkClient(io.vertx.ext.web.client.WebClient.wrap(vertx.createHttpClient()));
    context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServer.baseUrl());
    instanceIdMock = UUID.randomUUID().toString();
  }

  @Test
  public void shouldReturnInstanceLinkDtoCollectionWhenFound(TestContext testContext) {
    var async = testContext.async();
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
        .willReturn(ok(dtoJson)));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertTrue(result.isPresent());
      testContext.assertEquals(dtoJson, Json.encode(result.get()));
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenNotFound(TestContext testContext) {
    var async = testContext.async();

    stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
        .willReturn(notFound()));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertFalse(result.isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldFailFutureWhenServerError(TestContext testContext) {
    var async = testContext.async();

    stubFor(get(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
        .willReturn(serverError().withBody("Server error")));

    var future = instanceLinkClient.getLinksByInstanceId(instanceIdMock, context);

    future.whenComplete((result, throwable) -> {
      testContext.assertNull(result);
      testContext.assertNotNull(throwable);
      testContext.assertTrue(throwable.getCause() instanceof InstanceLinksException);
      async.complete();
    });
  }

  @Test
  public void shouldUpdateInstanceLinksSuccessfully(TestContext testContext) {
    var async = testContext.async();
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    stubFor(put(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
        .withRequestBody(equalToJson(new JsonObject(dtoJson).encode()))
        .willReturn(aResponse().withStatus(204)));

    var future = instanceLinkClient.updateInstanceLinks(instanceIdMock, dto, context);

    future.whenComplete((result, throwable) -> {
      testContext.assertNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldLogWarningWhenUpdateInstanceLinksFails(TestContext testContext) {
    var async = testContext.async();
    var dto = new InstanceLinkDtoCollection();
    var dtoJson = Json.encode(dto);

    stubFor(put(urlEqualTo(LINKS_API_PREFIX + instanceIdMock))
        .withRequestBody(equalToJson(new JsonObject(dtoJson).encode()))
        .willReturn(aResponse().withStatus(500).withBody("Update failed")));

    var future = instanceLinkClient.updateInstanceLinks(instanceIdMock, dto, context);

    future.whenComplete((result, throwable) -> {
      testContext.assertNull(throwable);
      async.complete();
    });
  }
}

