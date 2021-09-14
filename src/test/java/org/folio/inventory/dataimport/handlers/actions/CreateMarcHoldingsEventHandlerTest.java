package org.folio.inventory.dataimport.handlers.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

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
import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
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

  @Mock
  private Storage storage;
  @Mock
  HoldingsRecordCollection holdingsRecordsCollection;
  @Mock
  InstanceCollection instanceRecordCollection;
  private JsonObject mappingRules;
  private CreateMarcHoldingsEventHandler createMarcHoldingsEventHandler;
  private String instanceId;

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
    MockitoAnnotations.initMocks(this);
    MappingManager.clearReaderFactories();
    createMarcHoldingsEventHandler = new CreateMarcHoldingsEventHandler(storage);
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      instanceId = String.valueOf(UUID.randomUUID());
      Instance instance = new Instance(instanceId, String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      List<Instance> instanceList = Collections.singletonList(instance);
      MultipleRecords<Instance> result = new MultipleRecords<>(instanceList, 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
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
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_SRS_MARC_HOLDING_RECORD_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("id"));
    Assert.assertEquals(instanceId, new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("instanceId"));
    Assert.assertEquals(PERMANENT_LOCATION_ID, new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("permanentLocationId"));
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
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldNotProcessEventIfInstanceNotFoundByMarcHoldingsHrid() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
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
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("No instance id found for marc holdings with hrid: in00000000315", exception.getCause().getMessage());
  }

  @Test
  public void shouldThrowExceptionIfPermanentLocationIdIsNull() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
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
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
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
      Instance instance = new Instance(instanceId, String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      MultipleRecords<Instance> result = new MultipleRecords<>(Collections.singletonList(instance), 0);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
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
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_SRS_MARC_HOLDING_RECORD_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("id"));
    Assert.assertNull(new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("instanceId"));
    Assert.assertEquals(PERMANENT_LOCATION_ID, new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getString("permanentLocationId"));
  }

  @Test
  public void shouldNotProcessEventIfMarcHoldingDoesNotHave004Field() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
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
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
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
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())
        .withContentType(ACTION_PROFILE));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
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
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.DELETE)
      .withFolioRecord(HOLDINGS);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
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
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
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
}
