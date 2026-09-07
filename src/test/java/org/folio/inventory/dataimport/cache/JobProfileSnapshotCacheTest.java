package org.folio.inventory.dataimport.cache;

import static org.folio.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.HttpStatus.SC_NOT_FOUND;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.exceptions.CacheLoadingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class JobProfileSnapshotCacheTest extends BaseWireMockTest {

  private static final String TENANT_ID = "diku";
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots/.*";

  private final ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)));

  private ProfileSnapshotCache profileSnapshotCache;
  private Context context;

  @BeforeEach
  void setUp(Vertx vertx) {
    stubGetJson(PROFILE_SNAPSHOT_URL, Json.encode(jobProfileSnapshot));

    profileSnapshotCache = ProfileSnapshotCache.getInstance(vertx, vertx.createHttpClient());
    context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServerUrl());
  }

  @Test
  void shouldReturnProfileSnapshot(VertxTestContext testContext) {
    var optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isPresent());
      ProfileSnapshotWrapper actualProfileSnapshot = result.get();
      assertEquals(jobProfileSnapshot.getId(), actualProfileSnapshot.getId());
      assertFalse(actualProfileSnapshot.getChildSnapshotWrappers().isEmpty());
      assertEquals(jobProfileSnapshot.getChildSnapshotWrappers().getFirst().getId(),
        actualProfileSnapshot.getChildSnapshotWrappers().getFirst().getId());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(VertxTestContext testContext) {
    stubGetJson(PROFILE_SNAPSHOT_URL, SC_NOT_FOUND, "");

    var optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isEmpty());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(VertxTestContext testContext) {
    stubGetJson(PROFILE_SNAPSHOT_URL, SC_INTERNAL_SERVER_ERROR, "");

    var optionalFuture = profileSnapshotCache.get(jobProfileSnapshot.getId(), this.context);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(CacheLoadingException.class, err.getCause());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(VertxTestContext testContext) {
    var optionalFuture = profileSnapshotCache.get(null, this.context);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(NullPointerException.class, err);
      testContext.completeNow();
    })));
  }
}
