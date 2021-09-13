package org.folio.inventory.dataimport.cache;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class JobProfileSnapshotCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";

  private final Vertx vertx = Vertx.vertx();
  private final ProfileSnapshotCache profileSnapshotCache = new ProfileSnapshotCache(vertx,
    vertx.createHttpClient(new HttpClientOptions().setConnectTimeout(3000)), 3600);

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)));

  private Context context;

  @Before
  public void setUp() {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));

    context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServer.baseUrl());
  }

  @Test
  public void shouldReturnProfileSnapshot(TestContext context) {
    Async async = context.async();

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      ProfileSnapshotWrapper actualProfileSnapshot = ar.result().get();
      context.assertEquals(jobProfileSnapshot.getId(), actualProfileSnapshot.getId());
      context.assertFalse(actualProfileSnapshot.getChildSnapshotWrappers().isEmpty());
      context.assertEquals(jobProfileSnapshot.getChildSnapshotWrappers().get(0).getId(),
        actualProfileSnapshot.getChildSnapshotWrappers().get(0).getId());
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(TestContext context) {
    Async async = context.async();

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = profileSnapshotCache.get(null, this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
