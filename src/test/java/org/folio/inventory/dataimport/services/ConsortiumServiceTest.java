package org.folio.inventory.dataimport.services;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ConsortiumServiceTest extends BaseWireMockTest {

  private final String localTenant = "tenant";
  private final String centralTenantId = "consortiumTenant";
  private final String token = "token";
  private final UUID instanceId = UUID.randomUUID();
  private final String consortiumId = UUID.randomUUID().toString();

  private ConsortiumService consortiumService;
  private Context context;

  @BeforeEach
  void setUp(Vertx vertx) {
    context = EventHandlingUtil.constructContext(localTenant, token, WIRE_MOCK.baseUrl());
    var consortiumDataCache = new ConsortiumDataCache(vertx, vertx.createHttpClient());
    consortiumService = new ConsortiumServiceImpl(vertx.createHttpClient(), consortiumDataCache);

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(
        new JsonObject().put("centralTenantId", centralTenantId).put("consortiumId", consortiumId)));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(centralTenantId);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WIRE_MOCK.stubFor(
      WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
        .withHeader(CONTENT_TYPE.toString(), equalTo(HttpHeaderValues.APPLICATION_JSON.toString()))
        .willReturn(WireMock.created().withBody(Json.encode(sharingInstance))));
  }

  @Test
  void shouldReturnConsortiumCredentials(VertxTestContext testContext) {
    consortiumService.getConsortiumConfiguration(context)
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertTrue(result.isPresent());
        assertEquals(result.get().centralTenantId(), centralTenantId);
        assertEquals(result.get().consortiumId(), consortiumId);
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnEmptyOptionalIfNoConsortiumCredentialsFound(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("userTenants", new JsonArray())))));
    consortiumService.getConsortiumConfiguration(context)
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertTrue(result.isEmpty());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldShareInstance(VertxTestContext testContext) {
    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setSourceTenantId(centralTenantId);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);

    consortiumService.shareInstance(context, consortiumId, sharingInstance)
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertEquals(SharingStatus.COMPLETE, result.getStatus());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnEventProcessingExceptionIfSharedInstanceHasStatusError(VertxTestContext testContext) {
    String testError = "testError";

    SharingInstance resultingSharedInstance = new SharingInstance();
    resultingSharedInstance.setId(UUID.randomUUID());
    resultingSharedInstance.setSourceTenantId(centralTenantId);
    resultingSharedInstance.setInstanceIdentifier(instanceId);
    resultingSharedInstance.setTargetTenantId(localTenant);
    resultingSharedInstance.setStatus(SharingStatus.ERROR);
    resultingSharedInstance.setError(testError);

    WIRE_MOCK.stubFor(
      WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
        .willReturn(WireMock.ok().withBody(Json.encode(resultingSharedInstance))));

    SharingInstance incomingSharingInstance = new SharingInstance();
    incomingSharingInstance.setSourceTenantId(centralTenantId);
    incomingSharingInstance.setInstanceIdentifier(instanceId);
    incomingSharingInstance.setTargetTenantId(localTenant);

    consortiumService.shareInstance(context, consortiumId, incomingSharingInstance)
      .onComplete(testContext.failing(err -> testContext.verify(() -> {
        assertTrue(err.getMessage().contains(testError));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldCreateShadowInstance(VertxTestContext testContext) {
    consortiumService.createShadowInstance(context, instanceId.toString(),
        new ConsortiumConfiguration(centralTenantId, consortiumId))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        WIRE_MOCK.verify(
          WireMock.postRequestedFor(WireMock.urlEqualTo("/consortia/" + consortiumId + "/sharing/instances"))
            .withHeader(XOkapiHeaders.TENANT, equalTo(centralTenantId))
            .withHeader(XOkapiHeaders.TOKEN, equalTo(token))
            .withHeader(XOkapiHeaders.URL, equalTo(context.getOkapiLocation())));

        assertEquals(SharingStatus.COMPLETE, result.getStatus());
        testContext.completeNow();
      })));
  }
}
