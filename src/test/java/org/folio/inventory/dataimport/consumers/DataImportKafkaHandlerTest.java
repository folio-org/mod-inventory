package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportKafkaHandlerTest {

  private static final String TENANT_ID = "diku";

  @ClassRule
  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @Mock
  private Storage mockedStorage;

  @Mock
  private KafkaInternalCache kafkaInternalCache;

  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  private Vertx vertx = Vertx.vertx();
  private DataImportKafkaHandler dataImportKafkaHandler;

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withDataType(org.folio.JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withAction(CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(INSTANCE);

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    String[] hostAndPort = cluster.getBrokerList().split(":");
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .maxRequestSize(1048576)
      .build();

    HttpClient client = vertx.createHttpClient();
    dataImportKafkaHandler = new DataImportKafkaHandler(vertx, mockedStorage, client, kafkaInternalCache);
    EventManager.clearEventHandlers();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 1);
  }

  @Test
  public void shouldReturnSucceededFutureWhenProcessingCoreHandlerSucceeded(TestContext context) throws IOException {
    // given
    Async async = context.async();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl("localhost")
      .withToken("test-token")
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.completedFuture(new DataImportEventPayload()));
    EventManager.registerEventHandler(mockedEventHandler);

    Mockito.when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenProcessingCoreHandlerFailed(TestContext context) throws IOException {
    // given
    Async async = context.async();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withTenant("diku")
      .withOkapiUrl("localhost")
      .withToken("test-token")
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
    EventManager.registerEventHandler(mockedEventHandler);

    Mockito.when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

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
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl("localhost")
      .withToken("test-token")
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.completedFuture(new DataImportEventPayload()));
    EventManager.registerEventHandler(mockedEventHandler);

    Mockito.when(kafkaInternalCache.containsByKey("01")).thenReturn(true);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertNotEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

}

