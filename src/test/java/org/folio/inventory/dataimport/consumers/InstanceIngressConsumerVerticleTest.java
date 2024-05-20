package org.folio.inventory.dataimport.consumers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.folio.DataImportEventPayload;
import org.folio.inventory.InstanceIngressConsumerVerticle;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class InstanceIngressConsumerVerticleTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_NAME = "test-env";
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
    vertx.deployVerticle(InstanceIngressConsumerVerticle.class.getName(), options, deployAr -> async.complete());
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

    EventManager.clearEventHandlers();
    EventManager.registerEventHandler(mockedEventHandler);
  }

  @Test
  public void shouldSendEventWithProcessedEventPayloadWhenProcessingCoreHandlerSucceeded() throws InterruptedException {
    // given

    InstanceIngressPayload instanceIngressEventPayload = new InstanceIngressPayload()
      .withSourceType(InstanceIngressPayload.SourceType.BIBFRAME)
      .withSourceRecordObject("sourceRecord")
      .withSourceRecordIdentifier("sourceRecordId");
    InstanceIngressEvent event = new InstanceIngressEvent()
      .withId("01")
      .withEventType(InstanceIngressEvent.EventType.CREATE_INSTANCE)
      .withEventPayload(instanceIngressEventPayload);

    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_NAME, getDefaultNameSpace(), TENANT_ID, event.getEventType().value());

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
    vertx.close(ar -> {
      cluster.stop();
      async.complete();
    });
  }

}
