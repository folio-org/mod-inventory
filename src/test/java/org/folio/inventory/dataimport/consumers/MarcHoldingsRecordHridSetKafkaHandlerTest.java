package org.folio.inventory.dataimport.consumers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.HoldingsRecord;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

@RunWith(VertxUnitRunner.class)
public class MarcHoldingsRecordHridSetKafkaHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/holdings-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/holdings-record.json";
  private static final String HOLDINGS_PATH = "src/test/resources/handlers/holdings.json";

  @Mock
  private Storage mockedStorage;
  @Mock
  private HoldingsRecordCollection mockedHoldingsCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private KafkaInternalCache kafkaInternalCache;

  private JsonObject mappingRules;
  private org.folio.rest.jaxrs.model.Record record;
  private HoldingsRecord existingHoldingsRecord;
  private MarcHoldingsRecordHridSetKafkaHandler marcHoldingsRecordHridSetKafkaHandler;
  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(HOLDINGS_PATH)).mapTo(HoldingsRecord.class);
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingHoldingsRecord));
      return null;
    }).when(mockedHoldingsCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(mockedHoldingsCollection).update(any(HoldingsRecord.class), any(), any());

    marcHoldingsRecordHridSetKafkaHandler =
      new MarcHoldingsRecordHridSetKafkaHandler(new HoldingsUpdateDelegate(mockedStorage), kafkaInternalCache);
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
    payload.put("MARC_HOLDINGS", Json.encode(record));
    payload.put("MAPPING_RULES", mappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

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
  public void shouldReturnFailedFutureWhenPayloadHasNoMarcRecord(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("MAPPING_RULES", mappingRules.encode());
    payload.put("MAPPING_PARAMS", new JsonObject().encode());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

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

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcHoldingsRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
