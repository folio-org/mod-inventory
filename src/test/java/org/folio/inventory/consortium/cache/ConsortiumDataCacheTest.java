package org.folio.inventory.consortium.cache;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ConsortiumDataCacheTest extends BaseWireMockTest {

  private static final String TENANT_ID = "diku";
  private static final String USER_TENANTS_PATH = "/user-tenants?limit=1";
  private static final String USER_TENANTS_FIELD = "userTenants";
  private static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  private static final String CONSORTIUM_ID_FIELD = "consortiumId";

  private ConsortiumDataCache consortiumDataCache;
  private Map<String, String> okapiHeaders;

  @BeforeEach
  void setUp(Vertx vertx) {
    consortiumDataCache = new ConsortiumDataCache(vertx, vertx.createHttpClient());
    okapiHeaders = Map.of(
      XOkapiHeaders.TENANT, TENANT_ID,
      XOkapiHeaders.TOKEN, "token",
      XOkapiHeaders.URL, WIRE_MOCK.baseUrl());
  }

  @Test
  void shouldReturnConsortiumData(VertxTestContext testContext) {
    String expectedCentralTenantId = "mobius";
    String expectedConsortiumId = UUID.randomUUID().toString();

    var userTenantsCollection = new JsonObject()
      .put(USER_TENANTS_FIELD, new JsonArray()
        .add(new JsonObject()
          .put(CENTRAL_TENANT_ID_FIELD, expectedCentralTenantId)
          .put(CONSORTIUM_ID_FIELD, expectedConsortiumId)));

    WIRE_MOCK.stubFor(get(USER_TENANTS_PATH)
      .willReturn(WireMock.ok().withBody(userTenantsCollection.encodePrettily())));

    var future = consortiumDataCache.getConsortiumData(TENANT_ID, okapiHeaders);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isPresent());
      ConsortiumConfiguration consortiumConfig = result.get();
      assertEquals(expectedCentralTenantId, consortiumConfig.getCentralTenantId());
      assertEquals(expectedConsortiumId, consortiumConfig.getConsortiumId());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnEmptyOptionalIfSpecifiedTenantInHeadersIsNotInConsortium(VertxTestContext testContext) {
    var emptyUserTenantsCollection = new JsonObject()
      .put(USER_TENANTS_FIELD, JsonArray.of());

    WIRE_MOCK.stubFor(get(USER_TENANTS_PATH)
      .willReturn(WireMock.ok().withBody(emptyUserTenantsCollection.encodePrettily())));

    var future = consortiumDataCache.getConsortiumData(TENANT_ID, okapiHeaders);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isEmpty());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorOnConsortiumDataLoading(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(USER_TENANTS_PATH).willReturn(WireMock.serverError()));

    consortiumDataCache.getConsortiumData(TENANT_ID, okapiHeaders)
      .onComplete(testContext.failing(err -> testContext.verify(() -> {
        assertEquals("Error loading consortium data, tenantId: 'diku' response status: '500', body: 'null'",
          err.getMessage());
        testContext.completeNow();
      })));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedTenantIdIsNull(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(get(USER_TENANTS_PATH).willReturn(WireMock.serverError()));

    consortiumDataCache.getConsortiumData(null, okapiHeaders)
      .onComplete(testContext.failing(err -> testContext.verify(() -> {
        assertInstanceOf(NullPointerException.class, err);
        testContext.completeNow();
      })));
  }
}
