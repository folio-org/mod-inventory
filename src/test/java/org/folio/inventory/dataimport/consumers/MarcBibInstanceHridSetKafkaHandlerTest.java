package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Event;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler.JOB_EXECUTION_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class MarcBibInstanceHridSetKafkaHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";

  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @Mock
  private MappingMetadataCache mappingMetadataCache;

  private JsonObject mappingRules;
  private Record record;
  private Instance existingInstance;
  private MarcBibInstanceHridSetKafkaHandler marcBibInstanceHridSetKafkaHandler;
  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Instance instance = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedInstanceCollection).update(any(Instance.class), any(), any());

    Mockito.when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(mappingRules.encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    marcBibInstanceHridSetKafkaHandler = new MarcBibInstanceHridSetKafkaHandler(new InstanceUpdateDelegate(mockedStorage), kafkaInternalCache, mappingMetadataCache);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) throws IOException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put("MARC_BIB", Json.encode(record));
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcBibInstanceHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadHasNoMarcRecord(TestContext context) throws IOException {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcBibInstanceHridSetKafkaHandler.handle(kafkaRecord);

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
    Future<String> future = marcBibInstanceHridSetKafkaHandler.handle(kafkaRecord);

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
    payload.put("MARC_BIB", Json.encode(record));
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(true);

    // when
    Future<String> future = marcBibInstanceHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertNotEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }
}
