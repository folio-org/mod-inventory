package org.folio.inventory.dataimport.handlers;

import static org.folio.dataimport.util.marc.MarcConstants.FIELD_999;
import static org.folio.dataimport.util.marc.MarcConstants.INDICATOR_F;
import static org.folio.dataimport.util.marc.MarcConstants.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.inventory.kafka.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.FAIL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static support.KafkaUtility.checkKafkaEventSent;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.core.util.ReflectionUtil;
import org.folio.MappingMetadataDto;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.consumers.MarcBibUpdateKafkaConsumer;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.OptimisticLockingException;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.LinkUpdateReport;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import support.KafkaTest;
import support.TestUtil;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class MarcBibUpdateKafkaConsumerTest extends KafkaTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String INVALID_INSTANCE_ID = "02e54bce-9588-11ed-a1eb-0242ac120002";
  private static final String TENANT_ID = "test";

  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  private Record marcRecord;
  private Instance instance;
  private MarcBibUpdateKafkaConsumer marcBibUpdateKafkaConsumer;

  @BeforeEach
  @SneakyThrows
  void setUp() {
    instance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    marcRecord = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    marcRecord.getParsedRecord().withContent(JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).encode());

    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);

    when(mockedInstanceCollection.findByIdAndUpdate(not(eq(INVALID_INSTANCE_ID)), any(), any()))
      .thenReturn(instance);

    ReflectionUtil.setStaticFieldValue(MappingMetadataCache.class.getDeclaredField("instance"), mappingMetadataCache);

    when(mappingMetadataCache.getByRecordTypeBlocking(anyString(), any(Context.class), eq(MARC_BIB_RECORD_TYPE)))
      .thenReturn(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH)).encode())
        .withMappingParams(Json.encode(new MappingParameters()))));

    marcBibUpdateKafkaConsumer = new MarcBibUpdateKafkaConsumer(vertxAssistant.getVertx(), 100, kafkaConfig,
      new InstanceUpdateDelegate(mockedStorage));
  }

  @Test
  void shouldReturnSucceededFutureWithObtainedRecordKey(VertxTestContext testContext) {
    // given
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(marcRecord)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when + then
    vertxAssistant.getVertx().runOnContext(v -> {
      Future<String> future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);
      future.onComplete(testContext.succeeding(ar -> testContext.verify(() -> {
        assertEquals(expectedKafkaRecordKey, ar);
        verify(1);
        testContext.completeNow();
      })));
    });
  }

  @Test
  @SneakyThrows
  void shouldReturnSucceededFutureAfterHandlingOptimisticLockingError(VertxTestContext testContext) {
    // given
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(marcRecord)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));
    when(mockedInstanceCollection.findByIdAndUpdate(not(eq(INVALID_INSTANCE_ID)), any(), any()))
      .thenThrow(OptimisticLockingException.class).thenReturn(instance);

    // when + then
    vertxAssistant.getVertx().runOnContext(v -> {
      Future<String> future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);
      future.onComplete(testContext.succeeding(ar -> testContext.verify(() -> {
        verify(2);
        testContext.completeNow();
      })));
    });
  }

  @Test
  void shouldReturnFailedFutureWhenMappingRulesNotFound(VertxTestContext testContext) {
    // given
    when(mappingMetadataCache.getByRecordTypeBlocking(anyString(), any(Context.class), anyString()))
      .thenReturn(Optional.empty());

    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(marcRecord)
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when + then
    vertxAssistant.getVertx().runOnContext(v -> {
      Future<String> future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);
      future.onComplete(testContext.failing(cause -> testContext.verify(() -> {
        assertTrue(
          cause.getMessage().contains("MappingParameters and mapping rules snapshots were not found by jobId"));
        verifyNoInteractions(mockedInstanceCollection);
        Mockito.verify(mappingMetadataCache).getByRecordTypeBlocking(anyString(), any(Context.class), anyString());
        testContext.completeNow();
      })));
    });
  }

  @Test
  void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(VertxTestContext testContext) {
    // given
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(null)
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);

    // then
    future.onComplete(testContext.failing(cause -> testContext.verify(() -> {
      assertTrue(
        cause.getMessage().contains("Event message does not contain required data to update Instance by jobId"));
      verifyNoInteractions(mockedInstanceCollection);
      verifyNoInteractions(mappingMetadataCache);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotSendSuccessLinkReportEvent(VertxTestContext testContext) {
    // given
    var payload = new MarcBibUpdate()
      .withRecord(marcRecord)
      .withLinkIds(List.of(1, 2, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    var expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when + then
    vertxAssistant.getVertx().runOnContext(v -> {
      var future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);
      future.onComplete(testContext.succeeding(ar -> testContext.verify(() -> {
        assertEquals(expectedKafkaRecordKey, ar);
        var reports = checkKafkaEventSent(TENANT_ID, LINKS_STATS.topicName())
          .stream().map(ConsumerRecord::value).toList();
        var report = reports.stream()
          .map(value -> new JsonObject(value).mapTo(LinkUpdateReport.class))
          .filter(event -> payload.getJobId().equals(event.getJobId()))
          .findAny()
          .orElse(null);
        assertNull(report);
        testContext.completeNow();
      })));
    });
  }

  @Test
  @SneakyThrows
  void shouldSendFailedLinkReportEvent(VertxTestContext testContext) {
    // given
    var instanceId =
      AdditionalFieldsUtil.getValueFromDataField(marcRecord, FIELD_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I)
        .orElse(null);

    marcRecord.setId(INVALID_INSTANCE_ID);
    marcRecord.getExternalIdsHolder().setInstanceId(INVALID_INSTANCE_ID);
    var payload = new MarcBibUpdate()
      .withRecord(marcRecord)
      .withLinkIds(List.of(1, 3))
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));
    when(mockedInstanceCollection.findByIdAndUpdate(eq(INVALID_INSTANCE_ID), any(), any()))
      .thenThrow(new NotFoundException("Can't find Instance by id: " + marcRecord.getId()));

    // when + then
    vertxAssistant.getVertx().runOnContext(v -> {
      var future = marcBibUpdateKafkaConsumer.handle(kafkaRecord);
      future.onComplete(testContext.failing(cause -> {
        // sendEventToKafka() runs after promise.fail(); delay to let the Kafka I/O thread deliver
        // the message to the broker before the consumer polls.
        vertxAssistant.getVertx().setTimer(1000, timerId -> testContext.verify(() -> {
          var reports = checkKafkaEventSent(TENANT_ID, LINKS_STATS.topicName())
            .stream().map(ConsumerRecord::value).toList();
          assertFalse(reports.isEmpty());
          var report = reports.stream()
            .map(value -> new JsonObject(value).mapTo(LinkUpdateReport.class))
            .filter(event -> payload.getJobId().equals(event.getJobId()))
            .findAny()
            .orElse(null);
          assertNotNull(report);
          assertEquals(instanceId, report.getInstanceId());
          assertEquals(FAIL, report.getStatus());
          assertEquals(payload.getTenant(), report.getTenant());
          assertEquals(payload.getLinkIds(), report.getLinkIds());
          assertEquals("Can't find Instance by id: " + marcRecord.getId(), report.getFailCause());
          testContext.completeNow();
        }));
      }));
    });
  }

  @SneakyThrows
  private void verify(int n) {
    Mockito.verify(mappingMetadataCache, times(n))
      .getByRecordTypeBlocking(anyString(), any(Context.class), anyString());
    Mockito.verify(mockedStorage, times(n)).getInstanceCollection(any());
    Mockito.verify(mockedInstanceCollection, times(n)).findByIdAndUpdate(anyString(), any(), any());
  }
}
