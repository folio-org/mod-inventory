package org.folio.inventory.dataimport.consumers;

import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static support.KafkaUtility.checkKafkaEventSent;
import static support.KafkaUtility.sendEvent;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.DataImportConsumerVerticle;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import support.KafkaTest;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class DataImportConsumerVerticleTest extends KafkaTest {

  private static final String TENANT_ID = "diku";
  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";

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
            .withId(UUID.randomUUID().toString())
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  @Mock
  private EventHandler mockedEventHandler;

  @BeforeAll
  static void setUpClass() throws Exception {
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertxAssistant.getVertx(), 1);
    CancelledJobsIdsCache cancelledJobsIdsCache = new CancelledJobsIdsCache();

    CompletableFuture<String> deployFuture = new CompletableFuture<>();
    vertxAssistant.getVertx()
      .deployVerticle(() -> new DataImportConsumerVerticle(cancelledJobsIdsCache), deploymentOptions)
      .onComplete(ar -> {
        if (ar.succeeded()) { deployFuture.complete(ar.result()); } else {
          deployFuture.completeExceptionally(ar.cause());
        }
      });
    deployFuture.get(30, TimeUnit.SECONDS);
  }

  @BeforeEach
  void setUp() {
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    doAnswer(invocationOnMock -> {
      DataImportEventPayload eventPayload = invocationOnMock.getArgument(0);
      eventPayload.setCurrentNode(eventPayload.getCurrentNode().getChildSnapshotWrappers().getFirst());
      eventPayload.getEventsChain().add(eventPayload.getEventType());
      return CompletableFuture.completedFuture(eventPayload);
    }).when(mockedEventHandler).handle(any(DataImportEventPayload.class));

    stubGetJson(JOB_PROFILE_URL + "/.*", Json.encode(profileSnapshotWrapper));

    EventManager.clearEventHandlers();
    EventManager.registerEventHandler(mockedEventHandler);
  }

  @Test
  void shouldSendEventWithProcessedEventPayloadWhenProcessingCoreHandlerSucceeded() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServerUrl())
      .withToken("test-token")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));

    Map<String, String> headers = new HashMap<>();
    headers.put(DataImportHeaders.RECORD_ID, UUID.randomUUID().toString());
    headers.put(DataImportHeaders.CHUNK_ID, UUID.randomUUID().toString());

    // when
    sendEvent(headers, TENANT_ID, DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), event.getId(), Json.encode(event));

    // then
    var observedValues = checkKafkaEventSent(TENANT_ID, DI_COMPLETED.value(), 10000);

    assertEquals(1, observedValues.size());

    assertNotNull(observedValues.getFirst().headers().lastHeader(DataImportHeaders.RECORD_ID));
  }
}
