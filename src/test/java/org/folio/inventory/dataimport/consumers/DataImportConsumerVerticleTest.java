package org.folio.inventory.dataimport.consumers;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.DataImportConsumerVerticle;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportConsumerVerticleTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_NAME = "test-env";
  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";

  private static Vertx vertx;

  public static EmbeddedKafkaCluster cluster;

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
    Async async = context.async();
    cluster = provisionWith(defaultClusterConfig());
    cluster.start();
    String[] hostAndPort = cluster.getBrokerList().split(":");

    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .build();
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 1);

    vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(KAFKA_HOST, hostAndPort[0])
        .put(KAFKA_PORT, hostAndPort[1])
        .put(KAFKA_REPLICATION_FACTOR, "1")
        .put(KAFKA_ENV, KAFKA_ENV_NAME)
        .put(KAFKA_MAX_REQUEST_SIZE, "1048576"));
    vertx.deployVerticle(DataImportConsumerVerticle.class.getName(), options, deployAr -> async.complete());
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
  public void shouldSendEventWithProcessedEventPayloadWhenProcessingCoreHandlerSucceeded() throws InterruptedException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken("test-token")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>(Map.of("JOB_PROFILE_SNAPSHOT_ID", profileSnapshotWrapper.getId())));

    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_NAME, getDefaultNameSpace(), TENANT_ID, dataImportEventPayload.getEventType());
    Event event = new Event().withId("01").withEventPayload(Json.encode(dataImportEventPayload));
    KeyValue<String, String> record = new KeyValue<>("test-key", Json.encode(event));
    record.addHeader(RECORD_ID_HEADER, UUID.randomUUID().toString(), UTF_8);
    record.addHeader(CHUNK_ID_HEADER, UUID.randomUUID().toString(), UTF_8);

    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(record)).useDefaults();

    // when
    cluster.send(request);

    // then
    String observeTopic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_NAME, getDefaultNameSpace(), TENANT_ID, DI_COMPLETED.value());
    List<KeyValue<String, String>> observedValues = cluster.observe(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    assertEquals(1, observedValues.size());
    assertNotNull(observedValues.get(0).getHeaders().lastHeader(RECORD_ID_HEADER));
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(ar -> async.complete());
  }

}
