package org.folio.inventory.dataimport.consumers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.domain.dto.InstanceEvent;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;
import org.junit.After;
import org.junit.Before;
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
  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private MappingMetadataCache mappingMetadataCache;

  private JsonObject mappingRules;
  private Record record;
  private Instance existingInstance;
  private MarcBibUpdateKafkaHandler marcBibUpdateKafkaHandler;
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

    Mockito.when(mappingMetadataCache.getByRecordType(anyString(), any(Context.class), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(mappingRules.encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    marcBibUpdateKafkaHandler = new MarcBibUpdateKafkaHandler(new InstanceUpdateDelegate(mockedStorage), mappingMetadataCache);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) throws IOException {
    // given
    Async async = context.async();
    InstanceEvent payload = new InstanceEvent()
      .withRecord(Json.encode(record))
      .withType(InstanceEvent.EventType.UPDATE)
      .withTenant("diku")
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
      async.complete();
    });

    verify(mockedInstanceCollection, times(1)).findById(anyString(), any(), any());
    verify(mockedInstanceCollection, times(1)).update(any(Instance.class), any(), any());
    verify(mappingMetadataCache, times(1)).getByRecordType(anyString(), any(Context.class), anyString());
  }

  @Test
  public void shouldReturnFailedFutureWhenMappingRulesNotFound(TestContext context) throws IOException {
    // given
    Async async = context.async();
    Mockito.when(mappingMetadataCache.getByRecordType(anyString(), any(Context.class), anyString()))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    InstanceEvent payload = new InstanceEvent()
      .withRecord(Json.encode(record))
      .withType(InstanceEvent.EventType.UPDATE)
      .withTenant("diku")
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("MappingParameters and mapping rules snapshots were not found by jobId"));
      async.complete();
    });

    verify(mockedInstanceCollection, times(0)).findById(anyString(), any(), any());
    verify(mockedInstanceCollection, times(0)).update(any(Instance.class), any(), any());
    verify(mappingMetadataCache, times(1)).getByRecordType(anyString(), any(Context.class), anyString());
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {
    // given
    Async async = context.async();
    InstanceEvent payload = new InstanceEvent()
      .withRecord("")
      .withType(InstanceEvent.EventType.UPDATE)
      .withTenant("diku")
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = marcBibUpdateKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Event message does not contain required data to update Instance by jobId"));
      async.complete();
    });

    verify(mockedInstanceCollection, times(0)).findById(anyString(), any(), any());
    verify(mockedInstanceCollection, times(0)).update(any(Instance.class), any(), any());
    verify(mappingMetadataCache, times(0)).getByRecordType(anyString(), any(Context.class), anyString());
  }
}
