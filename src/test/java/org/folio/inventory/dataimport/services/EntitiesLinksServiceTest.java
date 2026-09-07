package org.folio.inventory.dataimport.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;
import org.folio.Link;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.services.EntitiesLinksService;
import org.folio.inventory.services.EntitiesLinksServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class EntitiesLinksServiceTest extends BaseWireMockTest {

  private static final String AUTHORITY_ID = "58600684-c647-408d-bf3e-756e9055a988";
  private static final String INSTANCE_AUTHORITY_LINKS_BODY = """
    {
      "links": [
        {
          "id": 1,
          "authorityId": "58600684-c647-408d-bf3e-756e9055a988",
          "authorityNaturalId": "test123",
          "instanceId": "eb89b292-d2b7-4c36-9bfc-f816d6f96418",
          "linkingRuleId": 1,
          "status": "ACTUAL"
        }
      ],
      "totalRecords": 1
    }
    """;
  private static final String INSTANCE_AUTHORITY_LINKS = """
    {
      "links": [
        {
          "authorityId": "58600684-c647-408d-bf3e-756e9055a988",
          "authorityNaturalId": "test123",
          "instanceId": "eb89b292-d2b7-4c36-9bfc-f816d6f96418",
          "linkingRuleId": 1,
          "status": "ACTUAL"
        }
      ]
    }
    """;
  private static final String LINKING_RULES_INSTANCE_AUTHORITY = """
    [
      {
        "id": 1,
        "bibField": "100",
        "authorityField": "100",
        "authoritySubfields": [
          "a",
          "b",
          "c",
          "d",
          "j",
          "q"
        ],
        "validation": {
          "existence": [
            {
              "t": false
            }
          ]
        },
        "autoLinkingEnabled": true
      }
    ]
    """;

  private final String localTenant = "tenant";
  private final String token = "token";
  private final String instanceId = UUID.randomUUID().toString();

  private EntitiesLinksService entitiesLinksService;
  private Context context;

  @BeforeEach
  void setUp(Vertx vertx) {
    entitiesLinksService = new EntitiesLinksServiceImpl(vertx, vertx.createHttpClient());
    context = EventHandlingUtil.constructContext(localTenant, token, WIRE_MOCK.baseUrl());
    JsonObject instanceAuthorityLinksResponse = new JsonObject(INSTANCE_AUTHORITY_LINKS_BODY);
    JsonArray linkingRulesResponse = new JsonArray(LINKING_RULES_INSTANCE_AUTHORITY);

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .willReturn(WireMock.ok().withBody(Json.encode(instanceAuthorityLinksResponse))));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/linking-rules/instance-authority"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(linkingRulesResponse))));

    WIRE_MOCK.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .withRequestBody(WireMock.equalToJson(INSTANCE_AUTHORITY_LINKS))
      .willReturn(WireMock.noContent()));
  }

  @Test
  void shouldReturnInstanceAuthorityLinks(VertxTestContext testContext) {
    entitiesLinksService.getInstanceAuthorityLinks(context, instanceId).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      assertFalse(ar.result().isEmpty());
      assertEquals(AUTHORITY_ID, ar.result().getFirst().getAuthorityId());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnConsortiumExceptionIfLinksResponseCodeIsNotOk(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .willReturn(WireMock.notFound()));
    entitiesLinksService.getInstanceAuthorityLinks(context, instanceId).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.failed());
      assertInstanceOf(ConsortiumException.class, ar.cause().getCause());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldPutInstanceAuthorityLinks(VertxTestContext testContext) {
    List<Link> instanceAuthorityLinks = List.of(
      Json.decodeValue(new JsonObject(INSTANCE_AUTHORITY_LINKS_BODY).getJsonArray("links").encode(), Link[].class));
    entitiesLinksService.putInstanceAuthorityLinks(context, instanceId, instanceAuthorityLinks)
      .onComplete(ar -> testContext.verify(() -> {
        assertTrue(ar.succeeded());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldReturnFailedIfExceptionDuringPutInstanceAuthorityLinks(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/links/instances/" + instanceId), true))
      .withRequestBody(WireMock.equalToJson(INSTANCE_AUTHORITY_LINKS))
      .willReturn(WireMock.serverError()));

    List<Link> instanceAuthorityLinks = List.of(
      Json.decodeValue(new JsonObject(INSTANCE_AUTHORITY_LINKS_BODY).getJsonArray("links").encode(), Link[].class));
    entitiesLinksService.putInstanceAuthorityLinks(context, instanceId, instanceAuthorityLinks)
      .onComplete(ar -> testContext.verify(() -> {
        assertTrue(ar.failed());
        assertInstanceOf(ConsortiumException.class, ar.cause().getCause());
        testContext.completeNow();
      }));
  }

  @Test
  void shouldReturnLinkingRules(VertxTestContext testContext) {
    entitiesLinksService.getLinkingRules(context).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      assertFalse(ar.result().isEmpty());
      assertEquals(1, ar.result().getFirst().getId());
      assertEquals("100", ar.result().getFirst().getBibField());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnConsortiumExceptionIfLinkingRulesResponseCodeIsNotOk(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/linking-rules/instance-authority"), true))
      .willReturn(WireMock.notFound()));
    entitiesLinksService.getLinkingRules(context).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.failed());
      assertInstanceOf(ConsortiumException.class, ar.cause().getCause());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldFailedWhenInvalidContext(VertxTestContext testContext) {
    context = EventHandlingUtil.constructContext(localTenant, token, "invalid");
    entitiesLinksService.getInstanceAuthorityLinks(context, instanceId).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.failed());
      assertInstanceOf(MalformedURLException.class, ar.cause().getCause());
      testContext.completeNow();
    }));
  }
}
