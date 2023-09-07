package org.folio.inventory.dataimport.consumers;

import api.ApiTestSuite;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.folio.inventory.ConsortiumInstanceSharingConsumerVerticle;
import org.folio.inventory.domain.instances.PublicationPeriod;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_INIT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.domain.instances.Instance.PUBLICATION_PERIOD_KEY;
import static org.folio.inventory.domain.instances.Instance.TAGS_KEY;
import static org.folio.inventory.domain.instances.Instance.TAG_LIST_KEY;
import static org.folio.inventory.domain.instances.PublicationPeriod.publicationPeriodToJson;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;

@RunWith(VertxUnitRunner.class)
public class ConsortiumInstanceSharingHandlerTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_NAME = "test-env";
  private static final String INSTANCES_URL = "/instance-storage/instances";
  private static final String RECORD_ID_HEADER = "recordId";

  private static Vertx vertx;
  public static EmbeddedKafkaCluster cluster;
  @Mock
  private EventHandler mockedEventHandler;
  private static KafkaConfig kafkaConfig;

  JsonObject instance = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("hrid", "in777")
    .put("title", "Long Way to a Small Angry Planet")
    .put("identifiers", new JsonArray().add(new JsonObject()
      .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
      .put("value", "9781473619777")))
    .put("contributors", new JsonArray().add(new JsonObject()
      .put("contributorNameTypeId", ApiTestSuite.getPersonalContributorNameType())
      .put("name", "Chambers, Becky")))
    .put("source", "Local")
    .put("administrativeNotes", new JsonArray().add("this is a note"))
    .put("instanceTypeId", ApiTestSuite.getTextInstanceType())
    .put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray().add("important")))
    .put(PUBLICATION_PERIOD_KEY, publicationPeriodToJson(new PublicationPeriod(1000, 2000)));

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

    kafkaConfig = KafkaConfig.builder()
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
    vertx.deployVerticle(ConsortiumInstanceSharingConsumerVerticle.class.getName(), options, deployAr -> async.complete());
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("http://mod-inventory:9403" + INSTANCES_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(instance))));

    HttpClient client = vertx.createHttpClient();

    EventManager.clearEventHandlers();
    EventManager.registerEventHandler(mockedEventHandler);
  }

  @Test
  public void shouldShareInstanceWithFOLIOSource() throws InterruptedException {

    // given
    String consortiumId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String data = "{\"id\":\"8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572\"," +
      "\"instanceIdentifier\":\"8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572\"," +
      "\"sourceTenantId\":\"consortium\"," +
      "\"targetTenantId\":\"university\"," +
      "\"status\":\"IN_PROGRESS\"}";

    KeyValue<String, String> kafkaRecord = new KeyValue<>(consortiumId, data);
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader("x-okapi-url", "http://mod-inventory:9403", UTF_8);

    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_NAME, getDefaultNameSpace(), TENANT_ID, CONSORTIUM_INSTANCE_SHARING_INIT.value());

    // when
    cluster.send(SendKeyValues.to(topic, Collections.singletonList(kafkaRecord)).useDefaults());

    // then
    String observeTopic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_NAME, getDefaultNameSpace(), TENANT_ID, CONSORTIUM_INSTANCE_SHARING_COMPLETE.value());
//    List<KeyValue<String, String>> observedValues = cluster.observe(ObserveKeyValues.on(observeTopic, 1)
//      .observeFor(30, TimeUnit.SECONDS)
//      .build());

    //assertEquals(0, observedValues.size());
    //assertNotNull(observedValues.get(0).getHeaders().lastHeader(RECORD_ID_HEADER));
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
