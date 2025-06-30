package org.folio.inventory.dataimport.handlers;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.KafkaTest;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportKafkaHandlerTest extends KafkaTest {
  private static final String TENANT_ID = "diku";
  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";

  @Mock
  private Storage mockedStorage;

  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private DataImportKafkaHandler dataImportKafkaHandler;

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create instance")
    .withDataType(org.folio.JobProfile.DataType.MARC);

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

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    HttpClient client = vertxAssistant.getVertx().createHttpClient();
    dataImportKafkaHandler = new DataImportKafkaHandler(vertxAssistant.getVertx(), mockedStorage, client,
      new ProfileSnapshotCache(vertxAssistant.getVertx(), client, 3600),
      kafkaConfig,
      MappingMetadataCache.getInstance(vertxAssistant.getVertx(), client),
      new ConsortiumDataCache(vertxAssistant.getVertx(), client));

    EventManager.clearEventHandlers();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertxAssistant.getVertx(), 1);
  }

  @Test
  public void shouldReturnSucceededFutureWhenProcessingCoreHandlerSucceeded(TestContext context) {
    // given
    String expectedPermissions = JsonArray.of("test-permission").encode();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken("test-token")
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    String expectedKafkaRecordKey = "test_key";
    List<KafkaHeader> headers = List.of(
      KafkaHeader.header(RECORD_ID_HEADER, UUID.randomUUID().toString()),
      KafkaHeader.header(CHUNK_ID_HEADER, UUID.randomUUID().toString()),
      KafkaHeader.header(PERMISSIONS, expectedPermissions)
    );
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(headers);

    EventHandler mockedEventHandler = mock(EventHandler.class);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    when(mockedEventHandler.handle(any(DataImportEventPayload.class)))
      .thenReturn(CompletableFuture.completedFuture(new DataImportEventPayload().withContext(new HashMap<>(Map.of("TEST_ENTITY_KEY", "TEST_ENTITY_VALUE")))));
    EventManager.registerEventHandler(mockedEventHandler);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(context.asyncAssertSuccess(actualKafkaRecordKey -> {
      context.assertEquals(expectedKafkaRecordKey, actualKafkaRecordKey);
      ArgumentCaptor<DataImportEventPayload> payloadCaptor = ArgumentCaptor.forClass(DataImportEventPayload.class);
      verify(mockedEventHandler).handle(payloadCaptor.capture());
      DataImportEventPayload payload = payloadCaptor.getValue();
      context.assertEquals(expectedPermissions, payload.getContext().get(PERMISSIONS));
    }));
  }

  @Test
  public void shouldReturnFailedFutureWhenProcessingCoreHandlerFailed(TestContext context) {
    // given
    Async async = context.async();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant("diku")
      .withOkapiUrl(mockServer.baseUrl())
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
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}

