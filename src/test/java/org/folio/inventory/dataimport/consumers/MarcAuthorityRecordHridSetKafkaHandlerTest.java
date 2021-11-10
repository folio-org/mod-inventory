package org.folio.inventory.dataimport.consumers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.inventory.dataimport.consumers.MarcHoldingsRecordHridSetKafkaHandler.JOB_EXECUTION_ID_KEY;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.Authority;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.AuthorityUpdateDelegate;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;

@RunWith(VertxUnitRunner.class)
public class MarcAuthorityRecordHridSetKafkaHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/authority-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/authority-record.json";
  private static final String AUTHORITY_PATH = "src/test/resources/handlers/authority.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  @Mock
  private Storage mockedStorage;
  @Mock
  private AuthorityRecordCollection mockedAuthorityCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private KafkaInternalCache kafkaInternalCache;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JsonObject mappingRules;
  private Record record;
  private Authority existingAuthority;
  private MarcAuthorityRecordHridSetKafkaHandler marcAuthorityRecordHridSetKafkaHandler;
  private AutoCloseable mocks;
  private final Vertx vertx = Vertx.vertx();
  private List<KafkaHeader> okapiHeaders;

  @Before
  public void setUp() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingAuthority = new JsonObject(TestUtil.readFileFromPath(AUTHORITY_PATH)).mapTo(Authority.class);
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getAuthorityRecordCollection(any(Context.class))).thenReturn(mockedAuthorityCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingAuthority));
      return null;
    }).when(mockedAuthorityCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Authority authority = invocationOnMock.getArgument(0);
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(authority));
      return null;
    }).when(mockedAuthorityCollection).update(any(Authority.class), any(), any());

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));

    MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx, vertx.createHttpClient(), 3600);
    marcAuthorityRecordHridSetKafkaHandler =
      new MarcAuthorityRecordHridSetKafkaHandler(new AuthorityUpdateDelegate(mockedStorage), kafkaInternalCache, mappingMetadataCache);

    this.okapiHeaders = List.of(
      KafkaHeader.header(OKAPI_TENANT_HEADER, "diku"),
      KafkaHeader.header(OKAPI_URL_HEADER, mockServer.baseUrl()));
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());
    payload.put("MARC_AUTHORITY", Json.encode(record));

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(okapiHeaders);

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcAuthorityRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadHasNoMarcRecord(TestContext context) {
    // given
    Async async = context.async();
    Map<String, String> payload = new HashMap<>();
    payload.put(JOB_EXECUTION_ID_KEY, UUID.randomUUID().toString());

    Event event = new Event().withId("01").withEventPayload(Json.encode(payload));
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcAuthorityRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {
    // given
    Async async = context.async();
    Event event = new Event().withId("01").withEventPayload(null);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));

    when(kafkaInternalCache.containsByKey("01")).thenReturn(false);

    // when
    Future<String> future = marcAuthorityRecordHridSetKafkaHandler.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
