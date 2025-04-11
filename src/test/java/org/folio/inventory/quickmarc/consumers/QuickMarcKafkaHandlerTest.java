package org.folio.inventory.quickmarc.consumers;

import org.folio.inventory.KafkaTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.folio.inventory.KafkaUtility.checkKafkaEventSent;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_ERROR;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_HOLDINGS_UPDATED;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.services.HoldingsCollectionService;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.Authority;
import org.folio.HoldingsRecord;
import org.folio.HoldingsType;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.consumers.QuickMarcKafkaHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;

@RunWith(VertxUnitRunner.class)
public class QuickMarcKafkaHandlerTest extends KafkaTest {

  private static final String TENANT_ID = "test";
  private static final String OKAPI_URL = "http://localhost";
  private static final String BIB_MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String BIB_RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String HOLDINGS_MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String HOLDINGS_RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_PATH = "src/test/resources/handlers/holdings.json";
  private static final String AUTHORITY_MAPPING_RULES_PATH = "src/test/resources/handlers/authority-rules.json";
  private static final String AUTHORITY_RECORD_PATH = "src/test/resources/handlers/authority-record.json";
  private static final String AUTHORITY_PATH = "src/test/resources/handlers/authority.json";

  @Mock
  private Storage mockedStorage;
  @Mock
  private HoldingsCollectionService holdingsCollectionService;
  @Mock
  private HoldingsRecordsSourceCollection sourceCollection;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsRecordCollection;
  @Mock
  private AuthorityRecordCollection mockedAuthorityRecordCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private OkapiHttpClient okapiHttpClient;

  private JsonObject bibMappingRules;
  private Record bibRecord;
  private Instance existingInstance;

  private JsonObject holdingsMappingRules;
  private Record holdingsRecord;
  private HoldingsRecord existingHoldings;

  private JsonObject authorityMappingRules;
  private Record authorityRecord;
  private Authority existingAuthority;

  private QuickMarcKafkaHandler handler;
  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
    bibMappingRules = new JsonObject(TestUtil.readFileFromPath(BIB_MAPPING_RULES_PATH));
    holdingsMappingRules = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_MAPPING_RULES_PATH));
    authorityMappingRules = new JsonObject(TestUtil.readFileFromPath(AUTHORITY_MAPPING_RULES_PATH));

    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    existingHoldings = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_PATH)).mapTo(HoldingsRecord.class);
    existingAuthority = new JsonObject(TestUtil.readFileFromPath(AUTHORITY_PATH)).mapTo(Authority.class);

    bibRecord = Json.decodeValue(TestUtil.readFileFromPath(BIB_RECORD_PATH), Record.class);
    bibRecord.getParsedRecord().withContent(JsonObject.mapFrom(bibRecord.getParsedRecord().getContent()).encode());

    holdingsRecord = Json.decodeValue(TestUtil.readFileFromPath(HOLDINGS_RECORD_PATH), Record.class);
    holdingsRecord.getParsedRecord().withContent(JsonObject.mapFrom(holdingsRecord.getParsedRecord().getContent()).encode());

    authorityRecord = Json.decodeValue(TestUtil.readFileFromPath(AUTHORITY_RECORD_PATH), Record.class);
    authorityRecord.getParsedRecord()
      .withContent(JsonObject.mapFrom(authorityRecord.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    var sourceId = String.valueOf(UUID.randomUUID());
    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsRecordCollection);
    when(mockedStorage.getAuthorityRecordCollection(any(Context.class))).thenReturn(mockedAuthorityRecordCollection);
    when(mockedStorage.getHoldingsRecordsSourceCollection(any(Context.class))).thenReturn(sourceCollection);
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(Future.succeededFuture(sourceId));

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

    doAnswer(invocationOnMock -> {
      Authority authority = invocationOnMock.getArgument(0);
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(authority));
      return null;
    }).when(mockedAuthorityRecordCollection).update(any(Authority.class), any(), any());

    when(okapiHttpClient.get(anyString())).thenReturn(
      CompletableFuture.completedFuture(new Response(200, new JsonObject().encode(), null, null)));
    when(okapiHttpClient.put(anyString(), any(JsonObject.class)))
      .thenReturn(CompletableFuture.completedFuture(new Response(204, null, null, null)));

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper =
      new PrecedingSucceedingTitlesHelper(context -> okapiHttpClient);
    handler =
      new QuickMarcKafkaHandler(vertxAssistant.getVertx(), mockedStorage, 100, kafkaConfig, precedingSucceedingTitlesHelper, holdingsCollectionService);

    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(XOkapiHeaders.TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(XOkapiHeaders.URL.toLowerCase(), OKAPI_URL)));
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldSendInstanceUpdatedEvent(TestContext context) {
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

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_INVENTORY_INSTANCE_UPDATED.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendHoldingsUpdatedEvent(TestContext context) {

    List<HoldingsType> holdings = new ArrayList<>();
    holdings.add(new HoldingsType().withName("testingnote$a"));
    MappingParameters mappingParameters = new MappingParameters();
    mappingParameters.withHoldingsTypes(holdings);
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_HOLDING");
    payload.put("MARC_HOLDING", Json.encode(holdingsRecord));
    payload.put("MAPPING_RULES", holdingsMappingRules.encode());
    payload.put("MAPPING_PARAMS", Json.encode(mappingParameters));
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_HOLDING)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_INVENTORY_HOLDINGS_UPDATED.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendAuthorityUpdatedEvent(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_AUTHORITY");
    payload.put("MARC_AUTHORITY", Json.encode(authorityRecord));
    payload.put("MAPPING_RULES", authorityMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_AUTHORITY)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingAuthority));
      return null;
    }).when(mockedAuthorityRecordCollection).findById(anyString(), any(), any());

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_INVENTORY_AUTHORITY_UPDATED.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendErrorEventWhenRecordIsNotExistInStorage(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_AUTHORITY");
    payload.put("MARC_AUTHORITY", Json.encode(authorityRecord));
    payload.put("MAPPING_RULES", authorityMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_AUTHORITY)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedAuthorityRecordCollection).findById(anyString(), any(), any());

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_ERROR.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendErrorEventWhenFailedToFetchRecordFromStorage(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_AUTHORITY");
    payload.put("MARC_AUTHORITY", Json.encode(authorityRecord));
    payload.put("MAPPING_RULES", authorityMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());
    payload.put("PARSED_RECORD_DTO", Json.encode(new ParsedRecordDto()
      .withRecordType(ParsedRecordDto.RecordType.MARC_AUTHORITY)
      .withRelatedRecordVersion("1")));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Unexpected failure", 500));
      return null;
    }).when(mockedAuthorityRecordCollection).findById(anyString(), any(), any());

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_ERROR.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldSendErrorEventWhenPayloadHasNoMarcRecord(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("RECORD_TYPE", "MARC_BIB");
    payload.put("MAPPING_RULES", bibMappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      var events = checkKafkaEventSent(TENANT_ID, QM_ERROR.name());
      context.assertNotNull(events);
      context.assertEquals(1, events.size());

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

    // when
    Future<String> future = handler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
