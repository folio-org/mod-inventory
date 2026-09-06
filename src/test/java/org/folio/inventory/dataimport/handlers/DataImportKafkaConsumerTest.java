package org.folio.inventory.dataimport.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.consumers.DataImportKafkaConsumer.PROFILE_SNAPSHOT_ID_KEY;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.JobProfile.DataType;
import org.folio.MappingProfile;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.inventory.dataimport.consumers.DataImportKafkaConsumer;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import support.KafkaTest;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DataImportKafkaConsumerTest extends KafkaTest {

  private static final String TENANT_ID = "diku";
  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withDataType(DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withAction(CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(INSTANCE);

  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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

  @Mock
  private Storage mockedStorage;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  private DataImportKafkaConsumer dataImportConsumer;
  private CancelledJobsIdsCache cancelledJobsIdCache;

  @BeforeEach
  void setUp() {
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    HttpClient client = vertxAssistant.getVertx().createHttpClient();
    cancelledJobsIdCache = CancelledJobsIdsCache.getInstance();
    dataImportConsumer = new DataImportKafkaConsumer(vertxAssistant.getVertx(), mockedStorage, client, kafkaConfig);

    EventManager.clearEventHandlers();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertxAssistant.getVertx(), 1);
  }

  @Test
  void shouldReturnSucceededFutureWhenProcessingCoreHandlerSucceeded(VertxTestContext testContext) {
    // given
    String expectedPermissions = JsonArray.of("test-permission").encode();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken("test-token")
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    String expectedKafkaRecordKey = "test_key";
    List<KafkaHeader> headers = List.of(
      KafkaHeader.header(DataImportHeaders.RECORD_ID, UUID.randomUUID().toString()),
      KafkaHeader.header(DataImportHeaders.CHUNK_ID, UUID.randomUUID().toString()),
      KafkaHeader.header(PERMISSIONS, expectedPermissions)
    );
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(headers);

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.completedFuture(
        new DataImportEventPayload().withContext(new HashMap<>(Map.of("TEST_ENTITY_KEY", "TEST_ENTITY_VALUE")))));
    EventManager.registerEventHandler(mockedEventHandler);

    // when
    Future<String> future = dataImportConsumer.handle(kafkaRecord);

    // then
    future.onComplete(testContext.succeeding(actualKafkaRecordKey -> testContext.verify(() -> {
      assertEquals(expectedKafkaRecordKey, actualKafkaRecordKey);
      ArgumentCaptor<DataImportEventPayload> payloadCaptor = ArgumentCaptor.forClass(DataImportEventPayload.class);
      verify(mockedEventHandler).handle(payloadCaptor.capture());
      DataImportEventPayload payload = payloadCaptor.getValue();
      assertEquals(expectedPermissions, payload.getContext().get(PERMISSIONS));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenProcessingCoreHandlerFailed(VertxTestContext testContext) {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken("test-token")
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
    EventManager.registerEventHandler(mockedEventHandler);

    // when
    Future<String> future = dataImportConsumer.handle(kafkaRecord);

    // then
    future.onComplete(testContext.failing(v -> testContext.verify(() -> {
      verify(mockedEventHandler).handle(any(DataImportEventPayload.class));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnSucceededFutureAndSkipEventProcessingIfEventPayloadContainsCancelledJobExecutionId(
    VertxTestContext testContext) {
    // given
    String expectedKafkaRecordKey = "test_key";
    String cancelledJobId = UUID.randomUUID().toString();
    cancelledJobsIdCache.put(cancelledJobId);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(cancelledJobId)
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(new HashMap<>(Map.of(PROFILE_SNAPSHOT_ID_KEY, profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    List<KafkaHeader> headers = List.of(
      KafkaHeader.header(DataImportHeaders.RECORD_ID, UUID.randomUUID().toString()),
      KafkaHeader.header(DataImportHeaders.CHUNK_ID, UUID.randomUUID().toString())
    );
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(headers);

    EventHandler mockedEventHandler = mock(EventHandler.class);
    EventManager.registerEventHandler(mockedEventHandler);

    // when
    Future<String> future = dataImportConsumer.handle(kafkaRecord);

    // then
    future.onComplete(testContext.succeeding(actualKafkaRecordKey -> testContext.verify(() -> {
      assertEquals(expectedKafkaRecordKey, actualKafkaRecordKey);
      verify(mockedEventHandler, never()).isEligible(any(DataImportEventPayload.class));
      verify(mockedEventHandler, never()).handle(any(DataImportEventPayload.class));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldProcessEventIfEventPayloadContainsCancelledJobExecutionIdButEventTypeIsDiSrsMarcBibRecordModifiedReadyForPostProcessing(
    VertxTestContext testContext) {
    // given
    String expectedKafkaRecordKey = "test_key";
    String cancelledJobId = UUID.randomUUID().toString();
    cancelledJobsIdCache.put(cancelledJobId);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(cancelledJobId)
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(new HashMap<>(Map.of(PROFILE_SNAPSHOT_ID_KEY, profileSnapshotWrapper.getId())))
      .withEventsChain(List.of(DI_SRS_MARC_BIB_RECORD_MATCHED.value()));
    assertEquals(
      DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(), dataImportEventPayload.getEventType());

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    List<KafkaHeader> headers = List.of(
      KafkaHeader.header(DataImportHeaders.RECORD_ID, UUID.randomUUID().toString()),
      KafkaHeader.header(DataImportHeaders.CHUNK_ID, UUID.randomUUID().toString())
    );
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(headers);

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.completedFuture(dataImportEventPayload));
    EventManager.registerEventHandler(mockedEventHandler);

    // when
    Future<String> future = dataImportConsumer.handle(kafkaRecord);

    // then
    future.onComplete(testContext.succeeding(actualKafkaRecordKey -> testContext.verify(() -> {
      assertEquals(expectedKafkaRecordKey, actualKafkaRecordKey);
      verify(mockedEventHandler).isEligible(any(DataImportEventPayload.class));
      verify(mockedEventHandler).handle(any(DataImportEventPayload.class));
      testContext.completeNow();
    })));
  }
}
