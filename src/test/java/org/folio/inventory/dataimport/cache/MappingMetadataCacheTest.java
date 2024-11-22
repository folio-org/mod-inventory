package org.folio.inventory.dataimport.cache;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.MappingMetadataDto;
import org.folio.inventory.domain.instances.InstanceCollection;
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
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class MappingMetadataCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String MARC_BIB_RECORD_TYPE = "marc-bib";

  private final Vertx vertx = Vertx.vertx();

  private final MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx,
    vertx.createHttpClient());

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
  public void shouldReturnMappingMetadataByRecordType(TestContext context) {
    Async async = context.async();
    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache
      .getByRecordType(mappingMetadata.getJobExecutionId(), this.context, MARC_BIB_RECORD_TYPE);
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
  public void shouldReturnMappingMetadataByRecordTypeBlocking() {
    var optionalMetadata = mappingMetadataCache
      .getByRecordTypeBlocking(mappingMetadata.getJobExecutionId(), this.context, MARC_BIB_RECORD_TYPE);

    assertTrue(optionalMetadata.isPresent());
    var actualMappingMetadata = optionalMetadata.get();
    assertEquals(mappingMetadata.getJobExecutionId(), actualMappingMetadata.getJobExecutionId());
    assertNotNull(actualMappingMetadata.getMappingParams());
    assertNotNull(actualMappingMetadata.getMappingRules());
    assertEquals(mappingMetadata.getMappingParams(), actualMappingMetadata.getMappingParams());
    assertEquals(mappingMetadata.getMappingRules(), actualMappingMetadata.getMappingRules());
  }

  @Test
  public void shouldReturnNoMappingMetadataWhenGetNotFoundByRecordTypeBlocking() {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    var optionalMetadata = mappingMetadataCache
      .getByRecordTypeBlocking(mappingMetadata.getJobExecutionId(), this.context, MARC_BIB_RECORD_TYPE);

    assertTrue(optionalMetadata.isEmpty());
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
  public void shouldReturnEmptyOptionalWhenGetNotFoundByRecordType(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.getByRecordType(mappingMetadata.getJobExecutionId(),
      this.context, MARC_BIB_RECORD_TYPE);

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
  public void shouldReturnFailedFutureWhenGetServerErrorByRecordType(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    Future<Optional<MappingMetadataDto>> optionalFuture = mappingMetadataCache.getByRecordType(mappingMetadata.getJobExecutionId(),
      this.context, MARC_BIB_RECORD_TYPE);

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

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNullByRecordType(TestContext context) {
    Async async = context.async();

    Future<Optional<MappingMetadataDto>> optionalFuture =
      mappingMetadataCache.getByRecordType(null, this.context, MARC_BIB_RECORD_TYPE);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
