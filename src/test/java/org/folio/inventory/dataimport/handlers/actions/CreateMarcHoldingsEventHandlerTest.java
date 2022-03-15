package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

import io.vertx.core.Future;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.HoldingsIdStorageService;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.MappingMetadataDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.HoldingsRecordsSource;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;

public class CreateMarcHoldingsEventHandlerTest {

  private static final String PARSED_CONTENT_WITH_004_FIELD = "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"},{\"004\":\"ybp7406411\"}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_CONTENT_WITH_PERMANENT_LOCATION_ID = "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"}, {\"852\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"b\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] }},   {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_HOLDINGS_RECORD = "src/test/resources/marc/parsed-holdings-record.json";
  private static final String PERMANENT_LOCATION_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-holdings-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  @Mock
  private Storage storage;
  @Mock
  HoldingsRecordCollection holdingsRecordsCollection;
  @Mock
  HoldingsRecordsSourceCollection holdingsRecordsSourceCollection;
  @Mock
  InstanceCollection instanceRecordCollection;
  @Mock
  private HoldingsIdStorageService holdingsIdStorageService;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JsonObject mappingRules;
  private CreateMarcHoldingsEventHandler createMarcHoldingsEventHandler;
  private String instanceId;
  private String sourceId;
  private String holdingsId;
  private String recordId;
  private Vertx vertx = Vertx.vertx();

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(HOLDINGS);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));


  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx, vertx.createHttpClient(), 3600);
    createMarcHoldingsEventHandler = new CreateMarcHoldingsEventHandler(storage, mappingMetadataCache, holdingsIdStorageService);
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      instanceId = String.valueOf(UUID.randomUUID());
      Instance instance = new Instance(instanceId, "2", String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      List<Instance> instanceList = Collections.singletonList(instance);
      MultipleRecords<Instance> result = new MultipleRecords<>(instanceList, 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      sourceId = String.valueOf(UUID.randomUUID());
      HoldingsRecordsSource source = new HoldingsRecordsSource();
      source.setId(sourceId);
      List<HoldingsRecordsSource> sourceList = Collections.singletonList(source);
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(sourceList, 1);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      recordId = String.valueOf(UUID.randomUUID());
      holdingsId = String.valueOf(UUID.randomUUID());
      RecordToEntity recordToHoldings = RecordToEntity.builder().recordId(recordId).entityId(holdingsId).build();
      return Future.succeededFuture(recordToHoldings);
    }).when(holdingsIdStorageService).store(any(), any(), any());

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    record.setId(recordId);
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject createdHoldings = new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));

    assertEquals(holdingsId, createdHoldings.getString("id"));
    assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    assertNotNull(createdHoldings.getString("id"));
    assertEquals(instanceId, createdHoldings.getString("instanceId"));
    assertEquals(sourceId, createdHoldings.getString("sourceId"));
    assertEquals(PERMANENT_LOCATION_ID, createdHoldings.getString("permanentLocationId"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsNull() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfFolioRecordIsNotMarcHoldings() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfChildSnapshotWrappersIsEmpty() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile)
      .withChildSnapshotWrappers(Collections.emptyList());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(10000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldNotProcessEventIfInstanceNotFoundByMarcHoldingsHrid() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("No instance id found for marc holdings with hrid: in00000000315", exception.getCause().getMessage());
  }

  @Test
  public void shouldThrowExceptionIfPermanentLocationIdIsNull() throws IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_004_FIELD));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("Can`t create Holding entity: 'permanentLocationId' is empty", exception.getCause().getMessage());
  }

  @Test
  public void shouldProcessEventIfInstanceFoundButTotalRecordsIsNotEqualOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    doAnswer(invocationOnMock -> {
      instanceId = String.valueOf(UUID.randomUUID());
      Instance instance = new Instance(instanceId, "2", String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      MultipleRecords<Instance> result = new MultipleRecords<>(Collections.singletonList(instance), 0);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    shouldProcessEventIfFieldFoundButTotalRecordsIsNotEqualOne("instanceId");
  }

  @Test
  public void shouldProcessEventIfSourceFoundButTotalRecordsIsNotEqualOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    doAnswer(invocationOnMock -> {
      sourceId = String.valueOf(UUID.randomUUID());
      HoldingsRecordsSource source = new HoldingsRecordsSource();
      source.setId(sourceId);
      List<HoldingsRecordsSource> sourceList = Collections.singletonList(source);
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(sourceList, 0);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    shouldProcessEventIfFieldFoundButTotalRecordsIsNotEqualOne("sourceId");
  }

  @Test
  public void shouldNotProcessEventIfMarcHoldingDoesNotHave004Field() throws IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_PERMANENT_LOCATION_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("The field 004 for marc holdings must be not null", exception.getCause().getMessage());
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfHoldingRecordIsInvalid() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Prelim item from MARC")
      .withIncomingRecordType(EntityType.MARC_HOLDINGS)
      .withExistingRecordType(EntityType.HOLDINGS)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(Lists.newArrayList(
          new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression"),
          new MappingRule().withPath("invalidField").withValue("invalidFieldValue"))));

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile)
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withProfileId(actionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(actionProfile)
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(mappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));


    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventWhenRecordToHoldingsFutureFails() throws ExecutionException, InterruptedException, TimeoutException {
    String recordId = "a0eb738a-c631-48cb-b36e-41cdcc83e2a4";

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(holdingsIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new Exception()));

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record();
    record.setId(recordId);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())
        .withContentType(ACTION_PROFILE));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventEvenIfDuplicatedInventoryStorageErrorExists() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(), any());

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    record.setId(recordId);
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void isEligibleShouldReturnTrue() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.DELETE)
      .withFolioRecord(HOLDINGS);
    ProfileSnapshotWrapper actionProfileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(actionProfileSnapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotHoldings() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ITEM);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertTrue(createMarcHoldingsEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value(), createMarcHoldingsEventHandler.getPostProcessingInitializationEventType());
  }

  public void shouldProcessEventIfFieldFoundButTotalRecordsIsNotEqualOne(String field) throws InterruptedException, ExecutionException, TimeoutException, IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    record.setId("a0eb738a-c631-48cb-b36e-41cdcc83e2a4");
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload actualDataImportEventPayload = future.get(10, TimeUnit.SECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("id"));
    Assert.assertNull(new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString(field));
    Assert.assertEquals(PERMANENT_LOCATION_ID, new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("permanentLocationId"));
  }
}
