package org.folio.inventory.client;

import static api.ApiTestSuite.TENANT_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.HttpStatus.SC_CREATED;
import static org.folio.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class SourceStorageRecordsClientWrapperTest extends BaseWireMockTest {

  private static final String RECORD = "Record";
  private static final String TOKEN = "stub-token";
  private static final String USER_ID = "12344";
  private static final String REQUEST_ID = "req12456";
  private static final String SOURCE_RECORDS_PATH = "/source-storage/records";

  private SourceStorageRecordsClientWrapper sourceStorageRecordsClientWrapper;
  private Record stubRecord;

  @BeforeEach
  void setUp(Vertx vertx) {
    var headers = FolioHeaders.builder()
      .connectionUrl(WIRE_MOCK.baseUrl())
      .token(TOKEN)
      .tenant(TENANT_ID)
      .userId(USER_ID)
      .requestId(REQUEST_ID);
    sourceStorageRecordsClientWrapper = new SourceStorageRecordsClientWrapper(headers, vertx.createHttpClient());

    stubRecord = new Record().withId(UUID.randomUUID().toString());

    WIRE_MOCK.stubFor(post(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH), true))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WIRE_MOCK.stubFor(put(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/" + stubRecord.getId()), true))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.ok()));

    WIRE_MOCK.stubFor(
      put(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/" + stubRecord.getId() + "/generation"), true))
        .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
        .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
        .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
        .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
        .willReturn(WireMock.ok()));

    WIRE_MOCK.stubFor(put(
      new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/" + stubRecord.getId() + "/suppress-from-discovery"),
        true))
      .withQueryParam("idType", equalTo(RECORD))
      .withQueryParam("suppress", equalTo("true"))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.ok()));
  }

  @Test
  void shouldPostSourceStorageRecords(VertxTestContext testContext) {
    var optionalFuture = sourceStorageRecordsClientWrapper.postSourceStorageRecords(stubRecord);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_CREATED, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutSourceStorageRecordsById(VertxTestContext testContext) {
    var optionalFuture = sourceStorageRecordsClientWrapper.putSourceStorageRecordsById(stubRecord.getId(), stubRecord);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutSourceStorageRecordsGenerationById(VertxTestContext testContext) {
    var optionalFuture = sourceStorageRecordsClientWrapper
      .putSourceStorageRecordsGenerationById(stubRecord.getId(), stubRecord);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutSourceStorageRecordsSuppressFromDiscoveryById(VertxTestContext testContext) {
    var optionalFuture = sourceStorageRecordsClientWrapper
      .putSourceStorageRecordsSuppressFromDiscoveryById(stubRecord.getId(), RECORD, true);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }
}
