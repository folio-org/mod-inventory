package org.folio.inventory.quickmarc.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ObserveKeyValues;

import org.folio.HoldingsRecord;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.consumers.QuickMarcKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_ERROR;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_HOLDINGS_UPDATED;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class QuickMarcKafkaHandlerTest {

  private static final String TENANT_ID = "test";
  private static final String OKAPI_URL = "http://localhost";
  private static final String BIB_MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String BIB_RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String HOLDINGS_MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String HOLDINGS_RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_PATH = "src/test/resources/handlers/holdings.json";

  @ClassRule
  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  private final Vertx vertx = Vertx.vertx();
  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsRecordCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @Mock
  private OkapiHttpClient okapiHttpClient;

  private JsonObject bibMappingRules;
  private Record bibRecord;
  private Instance existingInstance;
  private JsonObject holdingsMappingRules;
  private Record holdingsRecord;
  private HoldingsRecord existingHoldings;
  private QuickMarcKafkaHandler handler;
  private KafkaConfig kafkaConfig;
  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
    bibMappingRules = new JsonObject(TestUtil.readFileFromPath(BIB_MAPPING_RULES_PATH));
    holdingsMappingRules = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_MAPPING_RULES_PATH));
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    existingHoldings = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_PATH)).mapTo(HoldingsRecord.class);
    bibRecord = Json.decodeValue(TestUtil.readFileFromPath(BIB_RECORD_PATH), Record.class);
    holdingsRecord = Json.decodeValue(TestUtil.readFileFromPath(HOLDINGS_RECORD_PATH), Record.class);
    bibRecord.getParsedRecord().withContent(JsonObject.mapFrom(bibRecord.getParsedRecord().getContent()).encode());
    holdingsRecord.getParsedRecord().withContent(JsonObject.mapFrom(holdingsRecord.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsRecordCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingHoldings));
      return null;
    }).when(mockedHoldingsRecordCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Instance instance = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedInstanceCollection).update(any(Instance.class), any(), any());

    doAnswer(invocationOnMock -> {
      HoldingsRecord instance = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedHoldingsRecordCollection).update(any(HoldingsRecord.class), any(), any());

    when(okapiHttpClient.get(anyString())).thenReturn(
      CompletableFuture.completedFuture(new Response(200, new JsonObject().encode(), null, null)));
    when(okapiHttpClient.put(anyString(), any(JsonObject.class)))
      .thenReturn(CompletableFuture.completedFuture(new Response(204, null, null, null)));

    String[] hostAndPort = cluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .envId("env")
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .maxRequestSize(1048576)
      .build();

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(context -> okapiHttpClient);
    handler = new QuickMarcKafkaHandler(vertx, mockedStorage, 100, kafkaConfig, kafkaInternalCache, precedingSucceedingTitlesHelper);

    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(XOkapiHeaders.TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(XOkapiHeaders.URL.toLowerCase(), OKAPI_URL)));
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldSendInstanceUpdatedEvent(TestContext context) throws IOException,
    InterruptedException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_BIB");
    payload.put("MARC_BIB", Json.encode(bibRecord));
    payload.put("MAPPING_RULES", bibMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_BIB)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(payload)));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    String observeTopic =
      formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_INVENTORY_INSTANCE_UPDATED.name());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendHoldingsUpdatedEvent(TestContext context) throws IOException,
    InterruptedException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_HOLDING");
    payload.put("MARC_HOLDING", Json.encode(holdingsRecord));
    payload.put("MAPPING_RULES", holdingsMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_HOLDING)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(payload)));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    String observeTopic =
      formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_INVENTORY_HOLDINGS_UPDATED.name());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendErrorEventWhenPayloadHasNoMarcRecord(TestContext context)
    throws IOException, InterruptedException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_BIB");
    payload.put("MAPPING_RULES", bibMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());

    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(payload)));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    String observeTopic =
      formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_ERROR.name());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {
    // given
    Async async = context.async();
    Event event = new Event().withId("01").withEventPayload(null);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldNotHandleIfCacheAlreadyContainsThisEvent(TestContext context) throws IOException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_BIB");
    payload.put(Record.RecordType.MARC_BIB.value(), Json.encode(bibRecord));
    payload.put("MAPPING_RULES", bibMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());

    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(payload)));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(true);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertNotEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }
}
