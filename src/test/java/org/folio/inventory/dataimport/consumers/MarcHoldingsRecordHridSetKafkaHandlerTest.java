package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.handlers.actions.HoldingsRecordUpdateDelegate;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcHoldingsReaderFactory;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Consumer;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class MarcHoldingsRecordHridSetKafkaHandlerTest {

  private final String parsedRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"}, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"3\": \"test\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"245\":\"American Bar Association journal\" } ] }";
  private final String incomingSrsRecord = JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(parsedRecord))).encode();
  private final String existingHoldingsRecord = "{ \"id\": \"4d21d6ee-695b-441e-8f7a-e57b8eb940cb\", \"hrid\": \"ho00000000001\", \"instanceId\": \"ddd266ef-07ac-4117-be13-d418b8cd6902\", \"callNumber\": \"99101245\", \"copyNumber\": \"si990183\", \"numberOfItems\": \"2\" }\n";

  @Mock
  private Storage storageMock;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollectionMock;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecordMock;
  @Mock
  private KafkaInternalCache kafkaInternalCacheMock;
  private MarcHoldingsRecordHridSetKafkaHandler handler;

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Map Holding records from MARC Holdings")
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withPath("callNumber").withValue("042$3").withEnabled("true"))));
  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withContentType(MAPPING_PROFILE)
    .withContent(JsonObject.mapFrom(mappingProfile).getMap());

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    MappingManager.registerReaderFactory(new MarcHoldingsReaderFactory());
    MappingManager.registerWriterFactory(new HoldingWriterFactory());

    when(storageMock.getHoldingsRecordCollection(any(Context.class))).thenReturn(holdingsRecordCollectionMock);
    doAnswer(invocationOnMock -> {
      HoldingsRecord existingRecord = new JsonObject(existingHoldingsRecord).mapTo(HoldingsRecord.class);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingRecord));
      return null;
    }).when(holdingsRecordCollectionMock).findById(anyString(), any(), any());
    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordCollectionMock).update(any(HoldingsRecord.class), any(), any());
    Mockito.when(kafkaRecordMock.key()).thenReturn("testKey");

    this.handler = new MarcHoldingsRecordHridSetKafkaHandler(new HoldingsRecordUpdateDelegate(storageMock), kafkaInternalCacheMock);
  }

  @Test
  public void shouldMapHoldingAndReturnSucceededFuture(TestContext testContext) throws IOException {
    // given
    Async async = testContext.async();

    HashMap<String, String> context = new HashMap<>();
    context.put("holdingsId", UUID.randomUUID().toString());
    context.put(MARC_HOLDINGS.value(), incomingSrsRecord);
    context.put(HOLDINGS.value(), "{}");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);
    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(dataImportEventPayload)));
    when(kafkaRecordMock.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCacheMock.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecordMock);

    // then
    future.onComplete(ar -> {
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals("testKey", ar.result());
      async.complete();
    });
  }


  @Test
  public void shouldNotHandleIfCacheAlreadyContainsTheEvent(TestContext testContext) throws IOException {
    // given
    Async async = testContext.async();

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload();
    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(dataImportEventPayload)));
    when(kafkaRecordMock.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCacheMock.containsByKey("01")).thenReturn(true);

    // when
    Future<String> future = handler.handle(kafkaRecordMock);

    // then
    future.onComplete(ar -> {
      Assert.assertTrue(ar.succeeded());
      Assert.assertNull(ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadHasNoSrsMarcHoldingsRecord(TestContext testContext) throws IOException {
    // given
    Async async = testContext.async();

    HashMap<String, String> context = new HashMap<>();
    context.put("holdingsId", UUID.randomUUID().toString());
    context.put(HOLDINGS.value(), "{}");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);
    Event event = new Event().withId("01").withEventPayload(ZIPArchiver.zip(Json.encode(dataImportEventPayload)));
    when(kafkaRecordMock.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCacheMock.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = handler.handle(kafkaRecordMock);

    // then
    future.onComplete(ar -> {
      Assert.assertTrue(ar.failed());
      async.complete();
    });
  }
}
