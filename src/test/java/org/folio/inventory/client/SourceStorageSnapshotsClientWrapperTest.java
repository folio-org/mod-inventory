package org.folio.inventory.client;

import static api.ApiTestSuite.TENANT_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.HttpStatus.SC_CREATED;
import static org.folio.HttpStatus.SC_OK;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;
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
import org.folio.inventory.client.wrappers.SourceStorageSnapshotsClientWrapper;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class SourceStorageSnapshotsClientWrapperTest extends BaseWireMockTest {

  private static final String SOURCE_SNAPSHOTS_PATH = "/source-storage/snapshots";
  private static final String TOKEN = "token";
  private static final String USER_ID = "userId";
  private static final String REQUEST_ID = "requestId";

  private SourceStorageSnapshotsClientWrapper sourceStorageSnapshotsClientWrapper;
  private Snapshot stubSnapshot;

  @BeforeEach
  void setUp(Vertx vertx) {
    var headers = FolioHeaders.builder()
      .connectionUrl(WIRE_MOCK.baseUrl())
      .token(TOKEN)
      .tenant(TENANT_ID)
      .userId(USER_ID)
      .requestId(REQUEST_ID);
    sourceStorageSnapshotsClientWrapper =
      new SourceStorageSnapshotsClientWrapper(headers, vertx.createHttpClient());

    stubSnapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());

    WIRE_MOCK.stubFor(post(new UrlPathPattern(new RegexPattern(SOURCE_SNAPSHOTS_PATH), true))
      .withHeader(OKAPI_URL, equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WIRE_MOCK.stubFor(
      put(new UrlPathPattern(new RegexPattern(SOURCE_SNAPSHOTS_PATH + "/" + stubSnapshot.getJobExecutionId()), true))
        .withHeader(OKAPI_URL, equalTo(WIRE_MOCK.baseUrl()))
        .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
        .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
        .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
        .willReturn(WireMock.ok()));
  }

  @Test
  void shouldPostSourceStorageSnapshots(VertxTestContext testContext) {
    var optionalFuture = sourceStorageSnapshotsClientWrapper.postSourceStorageSnapshots(stubSnapshot);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_CREATED, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutSourceStorageSnapshotsByJobExecutionId(VertxTestContext testContext) {
    var optionalFuture = sourceStorageSnapshotsClientWrapper
      .putSourceStorageSnapshotsByJobExecutionId(stubSnapshot.getJobExecutionId(), stubSnapshot);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }
}
