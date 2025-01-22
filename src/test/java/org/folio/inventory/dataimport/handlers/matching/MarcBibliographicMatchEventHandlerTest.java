package org.folio.inventory.dataimport.handlers.matching;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RecordIdentifiersDto;
import org.folio.rest.jaxrs.model.RecordMatchingDto;
import org.folio.rest.jaxrs.model.RecordsIdentifiersCollection;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.Record.RecordType.MARC_BIB;
import static org.folio.inventory.dataimport.handlers.matching.AbstractMarcMatchEventHandler.CENTRAL_TENANT_ID_KEY;
import static org.folio.inventory.dataimport.handlers.matching.AbstractMarcMatchEventHandler.RECORDS_IDENTIFIERS_FETCH_LIMIT_PARAM;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.MATCH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class MarcBibliographicMatchEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\": \"01589ccm a2200373   4500\", \"fields\": [{\"001\": \"12345\"},{\"999\": {\"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [{\"s\": \"acf4f6e2-115c-4509-9d4c-536c758ef917\"},{\"i\": \"681394b4-10d8-4cb1-a618-0f9bd6152119\"}]}}]}";
  private static final String RECORDS_MATCHING_PATH = "/source-storage/records/matching";
  private static final String SOURCE_STORAGE_RECORDS_PATH_REGEX = "/source-storage/records/.{36}";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "mobius";
  private static final String TOKEN = "token";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final String USER_ID = UUID.randomUUID().toString();
  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";
  private static final String USER_ID_KEY = "userId";

  @ClassRule
  public static WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .notifier(new Slf4jNotifier(false))
      .dynamicPort());

  @Mock
  private ConsortiumService consortiumService;
  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsCollection;
  private AutoCloseable closeable;
  private final Vertx vertx = Vertx.vertx();
  private final Record expectedMatchedRecord = new Record()
    .withId(UUID.randomUUID().toString())
    .withRecordType(MARC_BIB)
    .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

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
  public void setUp() throws UnsupportedEncodingException {
    WireMock.reset();
    System.clearProperty(RECORDS_IDENTIFIERS_FETCH_LIMIT_PARAM);
    this.closeable = MockitoAnnotations.openMocks(this);
    Instance existingInstance = Instance.fromJson(new JsonObject().put("id", UUID.randomUUID().toString()));

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new ConsortiumConfiguration(CENTRAL_TENANT_ID, UUID.randomUUID().toString()))));
    when(mockedStorage.getInstanceCollection(any())).thenReturn(mockedInstanceCollection);
    when(mockedStorage.getHoldingsRecordCollection(any(Context.class))).thenReturn(mockedHoldingsCollection);
    when(mockedInstanceCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(existingInstance));

    doAnswer(invocationOnMock -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(new MultipleRecords<>(List.of(), 0)));
      return null;
    }).when(mockedHoldingsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any());

    matchMarcBibEventHandler = new MarcBibliographicMatchEventHandler(consortiumService, vertx.createHttpClient(), mockedStorage);
  }

  @After
  public void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void shouldMatchMarcBibAtNonConsortiumTenant(TestContext context) {
    Async async = context.async();
    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
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
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
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
      context.assertNull(payload.getContext().get(INSTANCE.value()));
      context.assertNull(payload.getContext().get(HOLDINGS.value()));
      async.complete();
    });
  }

  @Test
  public void shouldPopulatePayloadWithInstancesIdsOfMatchedRecordsIfMultipleRecordsMatchCriteriaAndNextProfileIsEligibleForMultiMatchResult(TestContext context) {
    Async async = context.async();
    List<String> instanceIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    List<RecordIdentifiersDto> recordsIdentifiers = instanceIds.stream()
      .map(id -> new RecordIdentifiersDto()
        .withRecordId(UUID.randomUUID().toString())
        .withExternalId(id))
      .toList();

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(recordsIdentifiers)
        .withTotalRecords(recordsIdentifiers.size())))));

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    DataImportEventPayload eventPayload = createEventPayloadWithSubmatchProfile(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> context.verify(v -> {
      context.assertNull(throwable);
      context.assertEquals(1, eventPayload.getEventsChain().size());
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), eventPayload.getEventType());
      context.assertNotNull(payload.getContext().get(INSTANCES_IDS_KEY));
      assertThat(new JsonArray(payload.getContext().get(INSTANCES_IDS_KEY)), containsInAnyOrder(instanceIds.toArray()));
      context.assertNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      async.complete();
    }));
  }

  @Test
  public void shouldRequestRecordsIdentifiersMultipleTimesIfMultipleRecordsMatchCriteriaOnParticularTenant(TestContext context) {
    Async async = context.async();
    int recordsIdentifiersLimit = 2;
    int totalRecordsIdentifiers = 5;
    int expectedRequestsNumber = Math.round(((float) totalRecordsIdentifiers) / ((float) recordsIdentifiersLimit));
    System.setProperty(RECORDS_IDENTIFIERS_FETCH_LIMIT_PARAM, String.valueOf(recordsIdentifiersLimit));

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(
          new RecordIdentifiersDto()
            .withRecordId("acf4f6e2-115c-4509-9d4c-536c758ef917")
            .withExternalId("681394b4-10d8-4cb1-a618-0f9bd6152119"),
          new RecordIdentifiersDto()
            .withRecordId("acf4f6e2-115c-4509-9d4c-536c758ef918")
            .withExternalId("681394b4-10d8-4cb1-a618-0f9bd615211a")))
        .withTotalRecords(totalRecordsIdentifiers)))));

    assertThat(totalRecordsIdentifiers, Matchers.greaterThan(recordsIdentifiersLimit));
    DataImportEventPayload eventPayload = createEventPayloadWithSubmatchProfile(TENANT_ID);

    MarcBibliographicMatchEventHandler eventHandler =
      new MarcBibliographicMatchEventHandler(consortiumService, vertx.createHttpClient(), mockedStorage);
    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> context.verify(v -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), eventPayload.getEventType());
      WireMock.verify(expectedRequestsNumber, postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withRequestBody(matchingJsonPath("$[?(@.returnTotalRecordsCount == true)]")));
      WireMock.verify(postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withRequestBody(matchingJsonPath("$[?(@.offset == 0)]")));
      WireMock.verify(postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withRequestBody(matchingJsonPath("$[?(@.offset == 2)]")));
      WireMock.verify(postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH))
        .withRequestBody(matchingJsonPath("$[?(@.offset == 4)]")));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnFailedFutureIfMultipleRecordsMatchCriteriaAndNextProfileNotEligibleForMultiMatchResult(TestContext context) {
    Async async = context.async();
    List<RecordIdentifiersDto> recordsIdentifiers = Stream.iterate(0, i -> i < 2, i -> ++i)
      .map(v -> new RecordIdentifiersDto()
        .withRecordId(UUID.randomUUID().toString())
        .withExternalId(UUID.randomUUID().toString()))
      .toList();

    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(recordsIdentifiers)
        .withTotalRecords(recordsIdentifiers.size())))));

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    context.assertTrue(eventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty());

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
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

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
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
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchMarcBibAtLocalTenantButMatchAtCentralTenant(TestContext context) {
    Async async = context.async();
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

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
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
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      async.complete();
    });
  }

  @Test
  public void shouldSearchRecordAtCentralTenantOnlyOnceIfCurrentTenantIsCentralTenant(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(CENTRAL_TENANT_ID);
    context.assertEquals(CENTRAL_TENANT_ID, eventPayload.getTenant());

    context.assertEquals(CENTRAL_TENANT_ID, eventPayload.getTenant());

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
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnFailedFutureIfMatchedRecordAtLocalTenantAndMatchedAtCentralTenant(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
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
    JsonObject invalidMatchProfileJson = new JsonObject()
      .put("invalidField", "val");

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.withCurrentNode(new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(invalidMatchProfileJson.getMap()));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldLoadInstanceAndHoldingFromLocalTenantIfMatchedRecordAtLocalTenant(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();
    HoldingsRecord existingHoldingsRecord = new HoldingsRecord().withId(UUID.randomUUID().toString());

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    doAnswer(invocationOnMock -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(new MultipleRecords<>(List.of(existingHoldingsRecord), 1)));
      return null;
    }).when(mockedHoldingsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any());

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      testContext.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      testContext.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      testContext.assertNotNull(payload.getContext().get(INSTANCE.value()));
      testContext.assertNotNull(payload.getContext().get(HOLDINGS.value()));
      verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(TENANT_ID)));
      verify(mockedStorage).getHoldingsRecordCollection(argThat(context -> context.getTenantId().equals(TENANT_ID)));
      async.complete();
    }));
  }

  @Test
  public void shouldLoadOnlyInstanceFromCentralTenantIfMatchedRecordAtCentralTenant(TestContext testContext) {
    Async async = testContext.async();
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

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      testContext.assertEquals(CENTRAL_TENANT_ID, payload.getContext().get(CENTRAL_TENANT_ID_KEY));
      testContext.assertEquals(1, payload.getEventsChain().size());
      testContext.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      testContext.assertNotNull(payload.getContext().get(INSTANCE.value()));
      verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
      verify(mockedStorage, never()).getHoldingsRecordCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
      async.complete();
    }));
  }

  @Test
  public void shouldLoadOnlyInstanceIfCurrentTenantIsCentralTenant(TestContext testContext) {
    Async async = testContext.async();
    WireMock.stubFor(post(RECORDS_MATCHING_PATH)
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(CENTRAL_TENANT_ID))
      .willReturn(WireMock.ok().withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(CENTRAL_TENANT_ID);
    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      testContext.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      testContext.assertNotNull(payload.getContext().get(INSTANCE.value()));
      verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
      verify(mockedStorage, never()).getHoldingsRecordCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
      async.complete();
    }));
  }

  @Test
  public void shouldNotLoadInstanceIfMatchedRecordHasNoInstanceId(TestContext testContext) {
    Async async = testContext.async();
    String parsedContentWithoutInstanceId = "{\"leader\": \"01589ccm a2200373   4500\", \"fields\": [{\"001\": \"12345\"}]}";
    Record matchedRecordWithoutInstanceId = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent(parsedContentWithoutInstanceId));

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(matchedRecordWithoutInstanceId))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      testContext.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      testContext.assertNull(payload.getContext().get(INSTANCE.value()));
      verify(mockedInstanceCollection, never()).findById(anyString());
      async.complete();
    }));
  }

  @Test
  public void shouldReturnFailedFutureIfPayloadHasNoMarcBibRecord(TestContext context) {
    Async async = context.async();
    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.getContext().remove(MARC_BIBLIOGRAPHIC.value());
    context.assertNull(eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfMatchProfileContainsInvalidMatchDetail(TestContext context) {
    Async async = context.async();
    MatchProfile matchProfileWithInvalidMatchDetail = new MatchProfile()
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withMatchDetails(List.of(new MatchDetail()
        .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
        .withExistingRecordType(MARC_BIBLIOGRAPHIC)
        .withMatchCriterion(EXACTLY_MATCHES)));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.withCurrentNode(new ProfileSnapshotWrapper()
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(matchProfileWithInvalidMatchDetail).getMap()));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((res, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfFailedToGetMatchedRecordsIdentifiers(TestContext context) {
    Async async = context.async();
    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.serverError()));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfGotErrorWhileLoadingMatchedRecordById(TestContext context) {
    Async async = context.async();
    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(UUID.randomUUID().toString())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.notFound()));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfFailedToLoadHoldingsForMatchedRecord(TestContext context) throws UnsupportedEncodingException {
    Async async = context.async();
    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", SC_INTERNAL_SERVER_ERROR));
      return null;
    }).when(mockedHoldingsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any());

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotSetHoldingIfMultipleHoldingsWereFoundForMatchedRecord(TestContext context) throws UnsupportedEncodingException {
    Async async = context.async();
    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);

    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    doAnswer(invocationOnMock -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(new MultipleRecords<>(List.of(new HoldingsRecord(), new HoldingsRecord()), 2)));
      return null;
    }).when(mockedHoldingsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any());

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), payload.getEventType());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      context.assertNull(payload.getContext().get(HOLDINGS.value()));
      async.complete();
    });
  }

  @Test
  public void shouldProcessPayloadContainingInstanceIdsRepresentingMultiMatchResultAndMatchMarcBib(TestContext context) {
    Async async = context.async();
    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    JsonArray instancesIds = JsonArray.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.getContext().put(INSTANCES_IDS_KEY, instancesIds.encode());

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> context.verify(v -> {
      context.assertNull(throwable);
      context.assertEquals(1, eventPayload.getEventsChain().size());
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), eventPayload.getEventType());
      context.assertNull(payload.getContext().get(INSTANCES_IDS_KEY));
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      WireMock.verify(1, postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH)));
      List<LoggedRequest> requests = WireMock.findAll(postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH)));
      RecordMatchingDto matchingDto = Json.decodeValue(requests.get(0).getBodyAsString(), RecordMatchingDto.class);
      context.assertEquals(2, matchingDto.getFilters().size());
      assertThat(matchingDto.getFilters().get(1).getValues(), containsInAnyOrder(instancesIds.getList().toArray()));
      async.complete();
    }));
  }

  @Test
  public void shouldMatchRecordAndTakeIntoAccountPreviouslyMatchedRecord(TestContext context) {
    Async async = context.async();
    Record previouslyMatchedRecord = new Record().withMatchedId(UUID.randomUUID().toString());
    when(consortiumService.getConsortiumConfiguration(any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    WireMock.stubFor(post(RECORDS_MATCHING_PATH).willReturn(WireMock.ok()
      .withBody(Json.encode(new RecordsIdentifiersCollection()
        .withIdentifiers(List.of(new RecordIdentifiersDto()
          .withRecordId(expectedMatchedRecord.getId())
          .withExternalId(UUID.randomUUID().toString())))
        .withTotalRecords(1)))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_STORAGE_RECORDS_PATH_REGEX), true))
      .willReturn(WireMock.ok().withBody(Json.encodePrettily(expectedMatchedRecord))));

    DataImportEventPayload eventPayload = createEventPayload(TENANT_ID);
    eventPayload.getContext().put(MATCHED_MARC_BIB_KEY, Json.encode(previouslyMatchedRecord));

    CompletableFuture<DataImportEventPayload> future = matchMarcBibEventHandler.handle(eventPayload);

    future.whenComplete((payload, throwable) -> context.verify(v -> {
      context.assertNull(throwable);
      context.assertEquals(1, eventPayload.getEventsChain().size());
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MATCHED.value(), eventPayload.getEventType());
      context.assertNotNull(payload.getContext().get(MATCHED_MARC_BIB_KEY));
      Record actualRecord = Json.decodeValue(payload.getContext().get(MATCHED_MARC_BIB_KEY), Record.class);
      context.assertEquals(expectedMatchedRecord.getId(), actualRecord.getId());
      context.assertNotNull(payload.getContext().get(INSTANCE.value()));
      WireMock.verify(1, postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH)));
      List<LoggedRequest> requests = WireMock.findAll(postRequestedFor(urlEqualTo(RECORDS_MATCHING_PATH)));
      RecordMatchingDto matchingRequest = Json.decodeValue(requests.get(0).getBodyAsString(), RecordMatchingDto.class);
      context.assertEquals(2, matchingRequest.getFilters().size());
      context.assertTrue(matchingRequest.getFilters().get(1).getValues().contains(previouslyMatchedRecord.getMatchedId()));
      async.complete();
    }));
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
    return createEventPayload(tenantId, null);
  }

  private DataImportEventPayload createEventPayloadWithSubmatchProfile(String tenantId) {
    ProfileSnapshotWrapper subMatchProfileWrapper = new ProfileSnapshotWrapper()
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withOrder(0)
      .withReactTo(MATCH)
      .withContent(JsonObject.mapFrom(matchProfile).getMap());

    return createEventPayload(tenantId, subMatchProfileWrapper);
  }

  private DataImportEventPayload createEventPayload(String tenantId, ProfileSnapshotWrapper nextProfileWrapper) {
    Record record = new Record()
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

    ProfileSnapshotWrapper matchProfileWrapper = new ProfileSnapshotWrapper()
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(JsonObject.mapFrom(matchProfile).getMap());

    if (nextProfileWrapper != null) {
      matchProfileWrapper.getChildSnapshotWrappers().add(nextProfileWrapper);
    }

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
