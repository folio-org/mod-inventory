package org.folio.inventory.dataimport.cache;

import static org.folio.HttpStatus.SC_BAD_REQUEST;
import static org.folio.HttpStatus.SC_NOT_FOUND;
import static org.folio.HttpStatus.SC_SERVER_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.MappingMetadataDto;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.exceptions.CacheLoadingException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class MappingMetadataCacheTest extends BaseWireMockTest {

  private static final String TENANT_ID = "diku";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata/.*";
  private static final String MARC_BIB_RECORD_TYPE = "marc-bib";

  private final MappingMetadataDto mappingMetadata = new MappingMetadataDto()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withMappingParams("params")
    .withMappingRules("rules");
  private final Context context = EventHandlingUtil.constructContext(TENANT_ID, "token", mockServerUrl());

  private MappingMetadataCache mappingMetadataCache;

  @BeforeEach
  void setUp(Vertx vertx) {
    stubGetJson(MAPPING_METADATA_URL, Json.encode(mappingMetadata));

    mappingMetadataCache = MappingMetadataCache.getInstance(vertx, true);
  }

  @Test
  void shouldReturnMappingMetadata(VertxTestContext testContext) {
    var optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), context);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isPresent());
      MappingMetadataDto actualMappingMetadata = result.get();
      assertEquals(mappingMetadata.getJobExecutionId(), actualMappingMetadata.getJobExecutionId());
      assertNotNull(actualMappingMetadata.getMappingParams());
      assertNotNull(actualMappingMetadata.getMappingRules());
      assertEquals(mappingMetadata.getMappingParams(), actualMappingMetadata.getMappingParams());
      assertEquals(mappingMetadata.getMappingRules(), actualMappingMetadata.getMappingRules());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnMappingMetadataByRecordType(VertxTestContext testContext) {
    var jobExecutionId = mappingMetadata.getJobExecutionId();
    var optionalFuture = mappingMetadataCache.getByRecordType(jobExecutionId, context, MARC_BIB_RECORD_TYPE);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isPresent());
      MappingMetadataDto actualMappingMetadata = result.get();
      assertEquals(jobExecutionId, actualMappingMetadata.getJobExecutionId());
      assertNotNull(actualMappingMetadata.getMappingParams());
      assertNotNull(actualMappingMetadata.getMappingRules());
      assertEquals(mappingMetadata.getMappingParams(), actualMappingMetadata.getMappingParams());
      assertEquals(mappingMetadata.getMappingRules(), actualMappingMetadata.getMappingRules());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnMappingMetadataByRecordTypeBlocking() {
    var jobExecutionId = mappingMetadata.getJobExecutionId();
    var optionalMetadata = mappingMetadataCache.getByRecordTypeBlocking(jobExecutionId, context, MARC_BIB_RECORD_TYPE);

    assertTrue(optionalMetadata.isPresent());
    var actualMappingMetadata = optionalMetadata.get();
    assertEquals(jobExecutionId, actualMappingMetadata.getJobExecutionId());
    assertNotNull(actualMappingMetadata.getMappingParams());
    assertNotNull(actualMappingMetadata.getMappingRules());
    assertEquals(mappingMetadata.getMappingParams(), actualMappingMetadata.getMappingParams());
    assertEquals(mappingMetadata.getMappingRules(), actualMappingMetadata.getMappingRules());
  }

  @Test
  void shouldReturnNoMappingMetadataWhenGetNotFoundByRecordTypeBlocking() {
    stubGetJson(MAPPING_METADATA_URL, SC_NOT_FOUND, "");

    var optionalMetadata = mappingMetadataCache
      .getByRecordTypeBlocking(mappingMetadata.getJobExecutionId(), context, MARC_BIB_RECORD_TYPE);

    assertTrue(optionalMetadata.isEmpty());
  }

  @Test
  void shouldThrowExceptionOnAttemptToGetByRecordTypeBlocking() {
    stubGetJson(MAPPING_METADATA_URL, SC_BAD_REQUEST, "");
    var jobId = mappingMetadata.getJobExecutionId();

    assertThrows(CacheLoadingException.class,
      () -> mappingMetadataCache.getByRecordTypeBlocking(jobId, context, MARC_BIB_RECORD_TYPE));
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(VertxTestContext testContext) {
    stubGetJson(MAPPING_METADATA_URL, SC_NOT_FOUND, "");

    var optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), context);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isEmpty());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundByRecordType(VertxTestContext testContext) {
    stubGetJson(MAPPING_METADATA_URL, SC_NOT_FOUND, "");

    var optionalFuture = mappingMetadataCache.getByRecordType(mappingMetadata.getJobExecutionId(),
      context, MARC_BIB_RECORD_TYPE);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isEmpty());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(VertxTestContext testContext) {
    stubGetJson(MAPPING_METADATA_URL, SC_SERVER_ERROR, "");

    var optionalFuture = mappingMetadataCache.get(mappingMetadata.getJobExecutionId(), context);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(CacheLoadingException.class, err.getCause());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorByRecordType(VertxTestContext testContext) {
    stubGetJson(MAPPING_METADATA_URL, SC_SERVER_ERROR, "");

    var optionalFuture = mappingMetadataCache.getByRecordType(mappingMetadata.getJobExecutionId(),
      context, MARC_BIB_RECORD_TYPE);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(CacheLoadingException.class, err.getCause());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(VertxTestContext testContext) {
    var optionalFuture = mappingMetadataCache.get(null, context);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(NullPointerException.class, err);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNullByRecordType(VertxTestContext testContext) {
    var optionalFuture = mappingMetadataCache.getByRecordType(null, context, MARC_BIB_RECORD_TYPE);

    optionalFuture.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertInstanceOf(NullPointerException.class, err);
      testContext.completeNow();
    })));
  }
}
