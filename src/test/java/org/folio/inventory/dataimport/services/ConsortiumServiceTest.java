package org.folio.inventory.dataimport.services;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.exceptions.NotFoundException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;


@RunWith(VertxUnitRunner.class)
public class ConsortiumServiceTest {
  private final Vertx vertx = Vertx.vertx();
  private final ConsortiumServiceImpl consortiumServiceImpl = new ConsortiumServiceImpl(vertx.createHttpClient());
  private final String localTenant = "tenant";
  private final String consortiumTenant = "consortiumTenant";
  private final String token = "token";
  private final UUID instanceId = UUID.randomUUID();
  private final String consortiumId = UUID.randomUUID().toString();
  private String baseUrl;
  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    baseUrl = mockServer.baseUrl();

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", consortiumTenant)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    JsonObject consortiumIdResponse = new JsonObject()
      .put("consortia", new JsonArray().add(new JsonObject().put("id", consortiumId)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(consortiumIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));
  }

  @Test
  public void shouldReturnCentralTenantId(TestContext testContext) {
    Async async = testContext.async();
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getCentralTenantId(context).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(ar.result(), consortiumTenant);
      async.complete();
    });
  }

  @Test
  public void shouldReturnNotFoundExceptionIfNoCentralTenantId(TestContext testContext) {
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("userTenants", new JsonArray())))));
    Async async = testContext.async();
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getCentralTenantId(context).onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(ar.cause().getCause() instanceof NotFoundException);
      async.complete();
    });
  }

  @Test
  public void shouldReturnConsortiumId(TestContext testContext) {
    Async async = testContext.async();
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getConsortiumId(context).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(ar.result(), consortiumId);
      async.complete();
    });
  }

  @Test
  public void shouldReturnNotFoundExceptionWhenNoConsortiumId(TestContext testContext) {
    Async async = testContext.async();
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("consortia", new JsonArray())))));
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getConsortiumId(context).onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(ar.cause().getCause() instanceof NotFoundException);
      async.complete();
    });
  }

  @Test
  public void shouldShareInstance(TestContext testContext) {
    Async async = testContext.async();

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);

    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.shareInstance(context, consortiumId, sharingInstance).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(ar.result().getStatus(), SharingStatus.COMPLETE);
      async.complete();
    });
  }

  @Test
  public void shouldReturnEventProcessingExceptionIfSharedInstanceHAveStatusError(TestContext testContext) {
    Async async = testContext.async();

    String testError = "testError";

    SharingInstance resultingSharedInstance = new SharingInstance();
    resultingSharedInstance.setId(UUID.randomUUID());
    resultingSharedInstance.setSourceTenantId(consortiumTenant);
    resultingSharedInstance.setInstanceIdentifier(instanceId);
    resultingSharedInstance.setTargetTenantId(localTenant);
    resultingSharedInstance.setStatus(SharingStatus.ERROR);
    resultingSharedInstance.setError(testError);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(resultingSharedInstance))));

    SharingInstance incomingSharingInstance = new SharingInstance();
    incomingSharingInstance.setSourceTenantId(consortiumTenant);
    incomingSharingInstance.setInstanceIdentifier(instanceId);
    incomingSharingInstance.setTargetTenantId(localTenant);

    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.shareInstance(context, consortiumId, incomingSharingInstance).onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      testContext.assertTrue(ar.cause().getMessage().contains(testError));
      async.complete();
    });
  }

  @Test
  public void shouldCreateShadowInstance(TestContext testContext) {
    Async async = testContext.async();

    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.createShadowInstance(context, instanceId.toString()).onComplete(ar -> {
      WireMock.verify(WireMock.getRequestedFor(WireMock.urlEqualTo("/user-tenants?limit=1"))
        .withHeader(TENANT_HEADER, WireMock.equalTo(localTenant))
        .withHeader(TOKEN_HEADER, WireMock.equalTo(token))
        .withHeader(OKAPI_URL_HEADER, WireMock.equalTo(context.getOkapiLocation())));

      WireMock.verify(WireMock.getRequestedFor(WireMock.urlEqualTo("/consortia"))
        .withHeader(TENANT_HEADER, WireMock.equalTo(consortiumTenant))
        .withHeader(TOKEN_HEADER, WireMock.equalTo(token))
        .withHeader(OKAPI_URL_HEADER, WireMock.equalTo(context.getOkapiLocation())));

      WireMock.verify(WireMock.postRequestedFor(WireMock.urlEqualTo("/consortia/" + consortiumId + "/sharing/instances"))
        .withHeader(TENANT_HEADER, WireMock.equalTo(consortiumTenant))
        .withHeader(TOKEN_HEADER, WireMock.equalTo(token))
        .withHeader(OKAPI_URL_HEADER, WireMock.equalTo(context.getOkapiLocation())));

      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(ar.result().getStatus(), SharingStatus.COMPLETE);
      async.complete();
    });
  }
}
