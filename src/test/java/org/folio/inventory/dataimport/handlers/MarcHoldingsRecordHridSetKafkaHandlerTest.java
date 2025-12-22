package org.folio.inventory.dataimport.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler.JOB_EXECUTION_ID_KEY;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.folio.HoldingsType;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.MappingMetadataDto;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.HoldingsRecord;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

@RunWith(VertxUnitRunner.class)
public class MarcHoldingsRecordHridSetKafkaHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_PATH = "src/test/resources/handlers/holdings.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  @Mock
  private Storage mockedStorage;
  @Mock
  private HoldingsCollectionService holdingsCollectionService;
  @Mock
  private HoldingsRecordsSourceCollection sourceCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JsonObject mappingRules;
  private org.folio.rest.jaxrs.model.Record marcRecord;
  private HoldingsRecord existingHoldingsRecord;
  private MarcHoldingsRecordHridSetKafkaHandler marcHoldingsRecordHridSetKafkaHandler;
  private AutoCloseable mocks;
  private Vertx vertx = Vertx.vertx();
  private List<KafkaHeader> okapiHeaders;

  @Before
  public void setUp() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_PATH)).mapTo(HoldingsRecord.class);
    marcRecord = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    marcRecord.getParsedRecord().withContent(JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    var sourceId = String.valueOf(UUID.randomUUID());
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsCollection);
    when(mockedStorage.getHoldingsRecordsSourceCollection(any(Context.class))).thenReturn(sourceCollection);
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(Future.succeededFuture(sourceId));
    when(holdingsCollectionService.getById(anyString(), any())).thenReturn(Future.succeededFuture(existingHoldingsRecord));
    when(holdingsCollectionService.update(any(HoldingsRecord.class), any()))
      .thenAnswer(invocationOnMock -> {
        HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
        return Future.succeededFuture(holdingsRecord);
      });

    List<HoldingsType> holdings = new ArrayList<>();
    holdings.add(new HoldingsType()
      .withName("testingnote$a")
      .withId("5f694a63-1bd4-4002-9f38-09170eb7aa62"));
    MappingParameters mappingParameters = new MappingParameters();
    mappingParameters.withHoldingsTypes(holdings);

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(mappingParameters))
        .withMappingRules(mappingRules.encode())))));

    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, vertx.createHttpClient(), true);
    marcHoldingsRecordHridSetKafkaHandler =
      new MarcHoldingsRecordHridSetKafkaHandler(new HoldingsUpdateDelegate(mockedStorage, holdingsCollectionService), mappingMetadataCache);

    this.okapiHeaders = List.of(
      KafkaHeader.header(OKAPI_TENANT_HEADER, "diku"),
      KafkaHeader.header(OKAPI_URL_HEADER, mockServer.baseUrl()));
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());
    payload.put("MARC_HOLDINGS", Json.encode(marcRecord));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(okapiHeaders);

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenOLErrorExist(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());
    payload.put("MARC_HOLDINGS", Json.encode(marcRecord));
    payload.put("CURRENT_RETRY_NUMBER", "1");

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(okapiHeaders);

    when(holdingsCollectionService.update(any(), any()))
      .thenReturn(Future.failedFuture(new OptimisticLockingException("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1")));

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadHasNoMarcRecord(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
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
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
