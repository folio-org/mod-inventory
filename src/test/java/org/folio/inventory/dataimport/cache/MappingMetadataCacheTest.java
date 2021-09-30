package org.folio.inventory.dataimport.cache;

import static com.github.tomakehurst.wiremock.client.WireMock.get;

import java.util.Optional;
import java.util.UUID;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.MappingMetadataDto;
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
public class MappingMetadataCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  private final Vertx vertx = Vertx.vertx();
  private final MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx,
    vertx.createHttpClient(new HttpClientOptions().setConnectTimeout(3000)), 3600);

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));


  MappingMetadataDto mappingMetadata = new MappingMetadataDto()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withMappingParams("params")
    .withMappingRules("rules");

  private Context context;

  @Before
  public void setUp() {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(mappingMetadata))));

    context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServer.baseUrl());
  }

  @Test
  public void shouldReturnMappingMetadata(TestContext context) {
    Async async = context.async();

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      MappingMetadataDto actualMappingMetadata = ar.result().get();
      context.assertEquals(mappingMetadata.getJobExecutionId(), actualMappingMetadata.getJobExecutionId());
      context.assertNotNull(actualMappingMetadata.getMappingParams());
      context.assertNotNull(actualMappingMetadata.getMappingRules());
      context.assertEquals(mappingMetadata.getMappingParams(), actualMappingMetadata.getMappingParams());
      context.assertEquals(mappingMetadata.getMappingRules(), actualMappingMetadata.getMappingRules());
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(TestContext context) {
    Async async = context.async();

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.get(null, this.context);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
