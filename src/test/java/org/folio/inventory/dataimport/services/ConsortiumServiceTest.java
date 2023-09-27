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
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;

@RunWith(VertxUnitRunner.class)
public class ConsortiumServiceTest {
  private final Vertx vertx = Vertx.vertx();
  private final ConsortiumServiceImpl consortiumServiceImpl = new ConsortiumServiceImpl(vertx.createHttpClient());
  private final String localTenant = "tenant";
  private final String centralTenantId = "consortiumTenant";
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
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", centralTenantId).put("consortiumId", consortiumId)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(centralTenantId);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .withHeader(CONTENT_TYPE.toString(), equalTo(APPLICATION_JSON))
      .willReturn(WireMock.created().withBody(Json.encode(sharingInstance))));
  }

  @Test
  public void shouldReturnConsortiumCredentials(TestContext testContext) {
    Async async = testContext.async();
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getConsortiumConfiguration(context).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().isPresent());
      testContext.assertEquals(ar.result().get().getCentralTenantId(), centralTenantId);
      testContext.assertEquals(ar.result().get().getConsortiumId(), consortiumId);
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalIfNoConsortiumCredentialsFound(TestContext testContext) {
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("userTenants", new JsonArray())))));
    Async async = testContext.async();
    Context context = EventHandlingUtil.constructContext(localTenant, token, baseUrl);
    consortiumServiceImpl.getConsortiumConfiguration(context).onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldShareInstance(TestContext testContext) {
    Async async = testContext.async();

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setSourceTenantId(centralTenantId);
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
  public void shouldReturnEventProcessingExceptionIfSharedInstanceHasStatusError(TestContext testContext) {
    Async async = testContext.async();

    String testError = "testError";

    SharingInstance resultingSharedInstance = new SharingInstance();
    resultingSharedInstance.setId(UUID.randomUUID());
    resultingSharedInstance.setSourceTenantId(centralTenantId);
    resultingSharedInstance.setInstanceIdentifier(instanceId);
    resultingSharedInstance.setTargetTenantId(localTenant);
    resultingSharedInstance.setStatus(SharingStatus.ERROR);
    resultingSharedInstance.setError(testError);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(resultingSharedInstance))));

    SharingInstance incomingSharingInstance = new SharingInstance();
    incomingSharingInstance.setSourceTenantId(centralTenantId);
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
    consortiumServiceImpl.createShadowInstance(context, instanceId.toString(), new ConsortiumConfiguration(centralTenantId, consortiumId)).onComplete(ar -> {
      WireMock.verify(WireMock.postRequestedFor(WireMock.urlEqualTo("/consortia/" + consortiumId + "/sharing/instances"))
        .withHeader(TENANT_HEADER, equalTo(centralTenantId))
        .withHeader(TOKEN_HEADER, equalTo(token))
        .withHeader(OKAPI_URL_HEADER, equalTo(context.getOkapiLocation())));

      testContext.assertTrue(ar.succeeded());
      testContext.assertEquals(ar.result().getStatus(), SharingStatus.COMPLETE);
      async.complete();
    });
  }
}
