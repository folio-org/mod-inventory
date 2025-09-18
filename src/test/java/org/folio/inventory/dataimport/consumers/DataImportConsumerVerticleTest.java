package org.folio.inventory.dataimport.consumers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.inventory.KafkaUtility.checkKafkaEventSent;
import static org.folio.inventory.KafkaUtility.sendEvent;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.DataImportConsumerVerticle;
import org.folio.inventory.KafkaTest;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class DataImportConsumerVerticleTest extends KafkaTest {
  private static final String TENANT_ID = "diku";
  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";

  @Mock
  private EventHandler mockedEventHandler;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

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
            .withId(UUID.randomUUID().toString())
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  @BeforeClass
  public static void setUpClass(TestContext context) {
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertxAssistant.getVertx(), 1);
    CancelledJobsIdsCache cancelledJobsIdsCache = new CancelledJobsIdsCache();
    vertxAssistant.getVertx().deployVerticle(() -> new DataImportConsumerVerticle(cancelledJobsIdsCache),
      deploymentOptions, context.asyncAssertSuccess());
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class))).thenReturn(true);
    doAnswer(invocationOnMock -> {
      DataImportEventPayload eventPayload = invocationOnMock.getArgument(0);
      eventPayload.setCurrentNode(eventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
      return CompletableFuture.completedFuture(eventPayload);
    }).when(mockedEventHandler).handle(any(DataImportEventPayload.class));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    EventManager.clearEventHandlers();
    EventManager.registerEventHandler(mockedEventHandler);
  }

  @Test
  public void shouldSendEventWithProcessedEventPayloadWhenProcessingCoreHandlerSucceeded()
    throws InterruptedException, ExecutionException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken("test-token")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));

    Map<String, String> headers = new HashMap<>();
    headers.put(RECORD_ID_HEADER, UUID.randomUUID().toString());
    headers.put(CHUNK_ID_HEADER, UUID.randomUUID().toString());

    // when
    sendEvent(headers, TENANT_ID,
      DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), event.getId(), Json.encode(event));

    // then
    var observedValues = checkKafkaEventSent(TENANT_ID, DI_COMPLETED.value(), 30000);

    assertEquals(1, observedValues.size());

    assertNotNull(observedValues.getFirst().headers().lastHeader(RECORD_ID_HEADER));
  }
}
