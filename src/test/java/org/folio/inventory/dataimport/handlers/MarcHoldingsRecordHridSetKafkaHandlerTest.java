package org.folio.inventory.dataimport.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler.JOB_EXECUTION_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.MappingMetadataDto;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import support.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.storage.Storage;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.HoldingsRecord;
import org.folio.rest.jaxrs.model.HoldingsType;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class MarcHoldingsRecordHridSetKafkaHandlerTest extends BaseWireMockTest {

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

  private org.folio.rest.jaxrs.model.Record marcRecord;
  private MarcHoldingsRecordHridSetKafkaHandler marcHoldingsRecordHridSetKafkaHandler;
  private List<KafkaHeader> okapiHeaders;

  @BeforeEach
  void setUp(Vertx vertx) {
    marcRecord = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    marcRecord.getParsedRecord().withContent(JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).encode());

    var sourceId = String.valueOf(UUID.randomUUID());
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsCollection);
    when(mockedStorage.getHoldingsRecordsSourceCollection(any(Context.class))).thenReturn(sourceCollection);
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(
      Future.succeededFuture(sourceId));
    HoldingsRecord existingHoldingsRecord =
      new JsonObject(TestUtil.readFileFromPath(HOLDINGS_PATH)).mapTo(HoldingsRecord.class);
    when(holdingsCollectionService.getById(anyString(), any())).thenReturn(Future.succeededFuture(
      existingHoldingsRecord));
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
        .withMappingRules(new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH)).encode())))));

    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, vertx.createHttpClient(), true);
    marcHoldingsRecordHridSetKafkaHandler =
      new MarcHoldingsRecordHridSetKafkaHandler(new HoldingsUpdateDelegate(mockedStorage, holdingsCollectionService),
        mappingMetadataCache);

    this.okapiHeaders = List.of(
      KafkaHeader.header(XOkapiHeaders.TENANT, "diku"),
      KafkaHeader.header(XOkapiHeaders.URL, WIRE_MOCK.baseUrl()));
  }

  @Test
  void shouldReturnSucceededFutureWithObtainedRecordKey(VertxTestContext testContext) {
    // given
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
    future.onComplete(ar -> testContext.verify(() -> {
      org.junit.jupiter.api.Assertions.assertTrue(ar.succeeded());
      org.junit.jupiter.api.Assertions.assertEquals(expectedKafkaRecordKey, ar.result());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFailedFutureWhenOLErrorExist(VertxTestContext testContext) {
    // given
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
      .thenReturn(Future.failedFuture(new OptimisticLockingException(
        "Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1")));

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      org.junit.jupiter.api.Assertions.assertTrue(ar.failed());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFailedFutureWhenPayloadHasNoMarcRecord(VertxTestContext testContext) {
    // given
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      org.junit.jupiter.api.Assertions.assertTrue(ar.failed());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(VertxTestContext testContext) {
    // given
    Event event = new Event().withId("01").withEventPayload(null);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      org.junit.jupiter.api.Assertions.assertTrue(ar.failed());
      testContext.completeNow();
    }));
  }
}
