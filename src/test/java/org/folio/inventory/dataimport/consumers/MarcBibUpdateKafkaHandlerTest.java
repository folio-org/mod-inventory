package org.folio.inventory.dataimport.consumers;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.FAIL;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.SUCCESS;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.ReadKeyValues;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.LinkUpdateReport;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.Record;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class MarcBibUpdateKafkaHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String INVALID_INSTANCE_ID = "02e54bce-9588-11ed-a1eb-0242ac120002";
  private static final String TENANT_ID = "test";
  private static Vertx vertx;
  private static EmbeddedKafkaCluster cluster;
  private static KafkaConfig kafkaConfig;

  @ClassRule
  public static RunTestOnContext rule = new RunTestOnContext();

  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  private Record record;
  private Instance instance;
  private MarcBibUpdateKafkaHandler marcBibUpdateKafkaHandler;
  private AutoCloseable mocks;

  @BeforeClass
  public static void beforeClass() {
    vertx = rule.vertx();

    cluster = provisionWith(defaultClusterConfig());
    cluster.start();
    String[] hostAndPort = cluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .envId("env")
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .maxRequestSize(1048576)
      .build();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(ar -> {
      cluster.stop();
      async.complete();
    });
  }

  @Before
  @SneakyThrows
  public void setUp() throws IOException {
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    instance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);

    Mockito.when(mockedInstanceCollection.findByIdAndUpdate(not(eq(INVALID_INSTANCE_ID)), any(), any()))
        .thenReturn(instance);

    Mockito.when(mappingMetadataCache.getByRecordTypeBlocking(anyString(), any(Context.class), eq(MARC_BIB_RECORD_TYPE)))
      .thenReturn(Optional.of(new MappingMetadataDto()
        .withMappingRules(mappingRules.encode())
        .withMappingParams(Json.encode(new MappingParameters()))));

    marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaHandler(vertx, 100, kafkaConfig, new InstanceUpdateDelegate(mockedStorage), mappingMetadataCache);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) {
    // given
    Async async = context.async();
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(record)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());

      context.verify(vo -> verify(vo, 1));
      async.complete();
    });
  }

  @Test
  @SneakyThrows
  public void shouldReturnSucceededFutureAfterHandlingOptimisticLockingError(TestContext context) {
    // given
    Async async = context.async();
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(record)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));
    Mockito.when(mockedInstanceCollection.findByIdAndUpdate(not(eq(INVALID_INSTANCE_ID)), any(), any()))
      .thenThrow(OptimisticLockingException.class).thenReturn(instance);

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      verify(null, 2);

      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenMappingRulesNotFound(TestContext context) {
    // given
    Async async = context.async();
    Mockito.when(mappingMetadataCache.getByRecordTypeBlocking(anyString(), any(Context.class), anyString()))
      .thenReturn(Optional.empty());

    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(record)
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("MappingParameters and mapping rules snapshots were not found by jobId"));
      verifyNoInteractions(mockedInstanceCollection);
      Mockito.verify(mappingMetadataCache).getByRecordTypeBlocking(anyString(), any(Context.class), anyString());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {
    // given
    Async async = context.async();
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(null)
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Event message does not contain required data to update Instance by jobId"));
      verifyNoInteractions(mockedInstanceCollection);
      verifyNoInteractions(mappingMetadataCache);
      async.complete();
    });
  }

  @Test
  public void shouldSendSuccessLinkReportEvent() throws InterruptedException {
    // given
    var topic = String.join(".", kafkaConfig.getEnvId(), TENANT_ID, LINKS_STATS.topicName());
    var expectedReportsCount = cluster.readValues(ReadKeyValues.from(topic)).size() + 1;
    var instanceId = AdditionalFieldsUtil.getValue(record, TAG_999, SUBFIELD_I)
      .orElse(null);

    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(record)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    vertx.runOnContext(v -> {
      List<String> reports;
      try {
        reports = cluster.observeValues(ObserveKeyValues.on(topic, expectedReportsCount)
          .observeFor(30, TimeUnit.SECONDS)
          .build());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      var report = reports.stream()
        .map(value -> new JsonObject(value).mapTo(LinkUpdateReport.class))
        .filter(event -> payload.getJobId().equals(event.getJobId()))
        .findAny()
        .orElse(null);

      Assert.assertNotNull(report);
      Assert.assertEquals(instanceId, report.getInstanceId());
      Assert.assertEquals(SUCCESS, report.getStatus());
      Assert.assertEquals(payload.getLinkIds(), report.getLinkIds());
      Assert.assertEquals(payload.getTenant(), report.getTenant());
      Assert.assertNull(report.getFailCause());
    });
  }

  @Test
  public void shouldSendFailedLinkReportEvent() throws InterruptedException {
    // given
    var topic = String.join(".", kafkaConfig.getEnvId(), TENANT_ID, LINKS_STATS.topicName());
    var expectedReportsCount = cluster.readValues(ReadKeyValues.from(topic)).size() + 1;
    var instanceId = AdditionalFieldsUtil.getValue(record, TAG_999, SUBFIELD_I)
      .orElse(null);

    record.setId(INVALID_INSTANCE_ID);
    record.getExternalIdsHolder().setInstanceId(INVALID_INSTANCE_ID);
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(record)
      .withLinkIds(List.of(1, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    vertx.runOnContext(v -> {
      List<String> reports;
      try {
        reports = cluster.observeValues(ObserveKeyValues.on(topic, expectedReportsCount)
          .observeFor(30, TimeUnit.SECONDS)
          .build());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      var report = reports.stream()
        .map(value -> new JsonObject(value).mapTo(LinkUpdateReport.class))
        .filter(event -> payload.getJobId().equals(event.getJobId()))
        .findAny()
        .orElse(null);

      Assert.assertNotNull(report);
      Assert.assertEquals(instanceId, report.getInstanceId());
      Assert.assertEquals(FAIL, report.getStatus());
      Assert.assertEquals(payload.getTenant(), report.getTenant());
      Assert.assertEquals(payload.getLinkIds(), report.getLinkIds());
      Assert.assertEquals("Can't find Instance by id: " + record.getId(), report.getFailCause());
    });
  }

  @SneakyThrows
  private void verify(Void vo, int n) {
    Mockito.verify(mappingMetadataCache, times(n)).getByRecordTypeBlocking(anyString(), any(Context.class), anyString());
    Mockito.verify(mockedStorage, times(n)).getInstanceCollection(any());
    Mockito.verify(mockedInstanceCollection, times(n)).findByIdAndUpdate(anyString(), any(), any());
  }
}
