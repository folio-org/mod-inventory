package org.folio.inventory.dataimport.consumers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.storage.Storage;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class InstanceIngressEventConsumerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String SRS_URL = "/source-storage/records";
  private static final String TENANT_ID = "test_tenant";
  private static final String MARC_RECORD = "src/test/resources/marc/parsedRecord.json";
  private static final String BIB_RECORD = "src/test/resources/handlers/bib-record.json";

  @Mock
  private Storage storage;
  @Mock
  InstanceCollection instanceRecordCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private InstanceIngressEventConsumer instanceIngressEventConsumer;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();

    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.toString())))));
    WireMock.stubFor(post(new UrlPattern(new RegexPattern(SRS_URL), true))
      .willReturn(WireMock.created().withBody(TestUtil.readFileFromPath(BIB_RECORD))));

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).add(any(), any(Consumer.class), any(Consumer.class));

    when(storage.getInstanceCollection(any(Context.class))).thenReturn(instanceRecordCollection);

    var vertx = Vertx.vertx();
    var httpClient = vertx.createHttpClient();
    instanceIngressEventConsumer = new InstanceIngressEventConsumer(storage, httpClient, new MappingMetadataCache(vertx, httpClient, 3600));
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) throws IOException {
    // given
    var async = context.async();

    var payload = new InstanceIngressPayload()
      .withSourceType(InstanceIngressPayload.SourceType.BIBFRAME)
      .withSourceRecordIdentifier(UUID.randomUUID().toString())
      .withSourceRecordObject(TestUtil.readFileFromPath(MARC_RECORD));
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventType(InstanceIngressEvent.EventType.CREATE_INSTANCE)
      .withEventPayload(payload)
      .withEventMetadata(new EventMetadata().withTenantId(TENANT_ID));

    var expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(List.of(
        KafkaHeader.header(XOkapiHeaders.URL.toLowerCase(), mockServer.baseUrl())
      )
    );

    // when
    var future = instanceIngressEventConsumer.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

}
