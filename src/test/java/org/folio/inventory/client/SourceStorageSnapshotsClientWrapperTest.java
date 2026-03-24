package org.folio.inventory.client;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import org.folio.inventory.client.wrappers.SourceStorageSnapshotsClientWrapper;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus.SC_CREATED;
import static com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus.SC_OK;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;

@RunWith(VertxUnitRunner.class)
public class SourceStorageSnapshotsClientWrapperTest {
  private final Vertx vertx = Vertx.vertx();
  private SourceStorageSnapshotsClientWrapper sourceStorageSnapshotsClientWrapper;
  private Snapshot stubSnapshot;
  private static final String TOKEN = "token";
  private static final String USER_ID = "userId";
  private static final String REQUEST_ID = "requestId";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    sourceStorageSnapshotsClientWrapper = new SourceStorageSnapshotsClientWrapper(mockServer.baseUrl(), TENANT_ID, TOKEN,
      USER_ID, REQUEST_ID, vertx.createHttpClient());

    stubSnapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern("/source-storage/snapshots"), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/" + stubSnapshot.getJobExecutionId()), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.ok()));
  }

  @Test
  public void shouldPostSourceStorageSnapshots(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = sourceStorageSnapshotsClientWrapper.postSourceStorageSnapshots(stubSnapshot);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_CREATED);
      async.complete();
    });
  }

  @Test
  public void shouldPutSourceStorageSnapshotsByJobExecutionId(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = sourceStorageSnapshotsClientWrapper
      .putSourceStorageSnapshotsByJobExecutionId(stubSnapshot.getJobExecutionId(), stubSnapshot);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_OK);
      async.complete();
    });
  }
}
