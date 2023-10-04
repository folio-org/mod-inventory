package org.folio.inventory.dataimport.services;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.EntityLink;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.consortium.services.EntitiesLinksService;
import org.folio.inventory.consortium.services.EntitiesLinksServiceImpl;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;;import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EntitiesLinksServiceTest {
  private final Vertx vertx = Vertx.vertx();
  private EntitiesLinksService entitiesLinksService = new EntitiesLinksServiceImpl(vertx.createHttpClient());

  private String baseUrl;
  private final String authorityId = "58600684-c647-408d-bf3e-756e9055a988";
  private final String INSTANCE_AUTHORITY_LINKS_BODY = "{\"links\":[{\"id\":\"1\",\"authorityId\":\"58600684-c647-408d-bf3e-756e9055a988\",\"authorityNaturalId\":\"test123\",\"instanceId\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\",\"linkingRuleId\":1,\"status\":\"ACTUAL\"}],\"totalRecords\":1}";
  private final String INSTANCE_AUTHORITY_LINKS = "{\"links\":[{\"id\":\"1\",\"authorityId\":\"58600684-c647-408d-bf3e-756e9055a988\",\"authorityNaturalId\":\"test123\",\"instanceId\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\",\"linkingRuleId\":1,\"status\":\"ACTUAL\"}]}";
  private final String localTenant = "tenant";
  private final String token = "token";
  private final String instanceId = UUID.randomUUID().toString();
  private Context context;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    baseUrl = mockServer.baseUrl();
    context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    JsonObject instanceAuthorityLinksResponse = new JsonObject(INSTANCE_AUTHORITY_LINKS_BODY);

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .willReturn(WireMock.ok().withBody(Json.encode(instanceAuthorityLinksResponse))));

    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
        .withRequestBody(WireMock.equalToJson(INSTANCE_AUTHORITY_LINKS))
      .willReturn(WireMock.ok().withBody(Json.encode(instanceAuthorityLinksResponse))));
  }

  @Test
  public void shouldReturnInstanceAuthorityLinks(TestContext testContext) {
    Async async = testContext.async();
    entitiesLinksService.getInstanceAuthorityLinks(context, instanceId).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(!ar.result().isEmpty());
      testContext.assertEquals(ar.result().get(0).getAuthorityId(), authorityId);
      async.complete();
    });
  }

  @Test
  public void shouldReturnConsortiumExceptionIfResponseCodeIsNotOK(TestContext testContext) {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .willReturn(WireMock.notFound()));
    entitiesLinksService.getInstanceAuthorityLinks(context, instanceId).onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(ar.cause().getCause() instanceof ConsortiumException);
      async.complete();
    });
  }

  @Test
  public void shouldPutInstanceAuthorityLinks(TestContext testContext) {
    Async async = testContext.async();
    List<EntityLink> instanceAuthorityLinks = List.of(Json.decodeValue(new JsonObject(INSTANCE_AUTHORITY_LINKS_BODY).getJsonArray("links").encode(), EntityLink[].class));
    entitiesLinksService.putInstanceAuthorityLinks(context, instanceId, instanceAuthorityLinks).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(!ar.result().isEmpty());
      testContext.assertEquals(ar.result().get(0).getAuthorityId(), authorityId);
      async.complete();
    });
  }
}
