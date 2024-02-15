package org.folio.inventory.dataimport.handlers.matching;

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
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RecordIdentifiersDto;
import org.folio.rest.jaxrs.model.RecordsIdentifiersCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.Record.RecordType.MARC_BIB;
import static org.folio.inventory.dataimport.handlers.matching.AbstractMarcMatchEventHandler.CENTRAL_TENANT_ID_KEY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class MarcBibliographicMatchEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\": \"01589ccm a2200373   4500\", \"fields\": [{\"001\": \"12345\"},{\"999\": {\"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [{\"s\": \"acf4f6e2-115c-4509-9d4c-536c758ef917\"},{\"i\": \"681394b4-10d8-4cb1-a618-0f9bd6152119\"}]}}]}";
  private static final String RECORDS_MATCHING_PATH = "/source-storage/records/matching";
  private static final String SOURCE_STORAGE_RECORDS_PATH = "/source-storage/records/";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "mobius";
  private static final String TOKEN = "token";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final String USER_ID = UUID.randomUUID().toString();
  private static final String USER_ID_KEY = "userId";

  @ClassRule
  public static WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .notifier(new Slf4jNotifier(false))
      .dynamicPort());

  @Mock
  private ConsortiumService consortiumService;
  private AutoCloseable closeable;
  private final Vertx vertx = Vertx.vertx();

  private MarcBibliographicMatchEventHandler matchMarcBibEventHandler;

  private final MatchProfile matchProfile = new MatchProfile()
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMatchDetails(List.of(new MatchDetail()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchCriterion(EXACTLY_MATCHES)
      .withIncomingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(List.of(
          new Field().withLabel("field").withValue("999"),
          new Field().withLabel("indicator1").withValue("f"),
          new Field().withLabel("indicator2").withValue("f"),
          new Field().withLabel("recordSubfield").withValue("s"))))
      .withExistingMatchExpression(new MatchExpression()
        .withDataValueType(VALUE_FROM_RECORD)
        .withFields(List.of(
          new Field().withLabel("field").withValue("999"),
          new Field().withLabel("indicator1").withValue("f"),
          new Field().withLabel("indicator2").withValue("f"),
          new Field().withLabel("recordSubfield").withValue("s"))))));

  @Before
  public void setUp() {
    WireMock.reset();
    this.closeable = MockitoAnnotations.openMocks(this);
    when(consortiumService.getConsortiumConfiguration(ArgumentMatchers.any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new ConsortiumConfiguration(CENTRAL_TENANT_ID, UUID.randomUUID().toString()))));

    matchMarcBibEventHandler = new MarcBibliographicMatchEventHandler(consortiumService, vertx.createHttpClient());
  }

  @After
  public void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void shouldMatchMarcBibAtNonConsortiumTenant(TestContext context) {
    Async async = context.async();
    when(consortiumService.getConsortiumConfiguration(ArgumentMatchers.any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    Record expectedMatchedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB);

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH + ".{36}"), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.getContext().put(USER_ID_KEY, USER_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      context.assertEquals(1, payload.getEventsChain().size());
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      context.assertEquals(USER_ID, payload.getAdditionalProperties().get(USER_ID_KEY));
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchMarcBibIfNoRecordsMatchingCriteria(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of())
        .withTotalRecords(0)))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), payload.getEventType());
      context.assertNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      context.assertEquals(1, payload.getEventsChain().size());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfMultipleRecordsMatchCriteriaOnLocalTenant(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(
          new RecordIdentifiersDto()
            .withRecordId("acf4f6e2-115c-4509-9d4c-536c758ef917")
            .withExternalId("681394b4-10d8-4cb1-a618-0f9bd6152119"),
          new RecordIdentifiersDto()
            .withRecordId("acf4f6e2-115c-4509-9d4c-536c758ef918")
            .withExternalId("681394b4-10d8-4cb1-a618-0f9bd615211a")))
        .withTotalRecords(1)))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> context.verify(v -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      context.assertEquals(1, eventPayload.getEventsChain().size());
      WireMock.verify(0, postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(CENTRAL_TENANT_ID)));
      async.complete();
    }));
  }

  @Test
  public void shouldNotMatchMarcBibIfIncomingRecordHasNoSpecifiedIncomingField(TestContext context) {
    Async async = context.async();
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(List.of(new MatchDetail()
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingRecordType(MARC_BIBLIOGRAPHIC)
        .withMatchCriterion(EXACTLY_MATCHES)
        .withIncomingMatchExpression(new MatchExpression()
          .withDataValueType(VALUE_FROM_RECORD)
          .withFields(List.of(
            new Field().withLabel("field").withValue("900"),
            new Field().withLabel("indicator1").withValue(""),
            new Field().withLabel("indicator2").withValue(""),
            new Field().withLabel("recordSubfield").withValue("a"))))
        .withExistingMatchExpression(new MatchExpression()
          .withDataValueType(VALUE_FROM_RECORD)
          .withFields(List.of(
            new Field().withLabel("field").withValue("900"),
            new Field().withLabel("indicator1").withValue(""),
            new Field().withLabel("indicator2").withValue(""),
            new Field().withLabel("recordSubfield").withValue("a"))))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.withCurrentNode(new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(matchProfile));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), payload.getEventType());
      context.assertEquals(1, payload.getEventsChain().size());
      async.complete();
    });
  }

  @Test
  public void shouldMatchAtLocalTenantAndNotMatchAtCentralTenant(TestContext context) {
    Async async = context.async();
    Record expectedMatchedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB);

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(CENTRAL_TENANT_ID))
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of())
        .withTotalRecords(0)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH + ".{36}"), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      context.assertNull(payload.getContext().get(CENTRAL_TENANT_ID_KEY));
      context.assertEquals(1, payload.getEventsChain().size());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchMarcBibAtLocalTenantButMatchAtCentralTenant(TestContext context) {
    Async async = context.async();
    Record expectedMatchedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of())
        .withTotalRecords(0)))));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(CENTRAL_TENANT_ID))
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH + ".{36}"), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      context.assertEquals(CENTRAL_TENANT_ID, payload.getContext().get(CENTRAL_TENANT_ID_KEY));
      context.assertEquals(1, payload.getEventsChain().size());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      async.complete();
    });
  }

  @Test
  public void shouldSearchRecordAtCentralTenantOnlyOnceIfCurrentTenantIsCentralTenant(TestContext context) {
    Async async = context.async();
    Record expectedMatchedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB);

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH + ".{36}"), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(CENTRAL_TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> context.verify(v -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      context.assertEquals(1, payload.getEventsChain().size());
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      WireMock.verify(1, postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(CENTRAL_TENANT_ID)));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnFailedFutureIfMatchedRecordAtLocalTenantAndMatchedAtCentralTenant(TestContext context) {
    Async async = context.async();
    Record expectedMatchedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB);

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH + ".{36}"), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfFailedToDeserializeMatchProfile(TestContext context) {
    Async async = context.async();
    Record record = new Record()
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

    JsonObject invalidMatchProfileJson = new JsonObject()
      .put("invalidField", "val");

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(invalidMatchProfileJson.getMap()))
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      }});

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnTrueIfHandlerIsEligibleForEventPayload() {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withProfileId(matchProfile.getId())
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()));

    assertTrue(matchMarcBibEventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseIfHandlerIsNotEligibleForPayload() {
    MatchProfile matchProfile = new MatchProfile()
      .withIncomingRecordType(MARC_AUTHORITY)
      .withExistingRecordType(MARC_AUTHORITY);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(matchProfile).getMap()));

    assertFalse(matchMarcBibEventHandler.isEligible(eventPayload));
  }

  private DataImportEventPayload createEventPayload(String tenantId) {
    Record record = new Record()
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper()
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(matchProfile).getMap());

    return new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(tenantId)
      .withToken(TOKEN)
      .withCurrentNode(matchProfileWrapper)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      }});
  }

}
