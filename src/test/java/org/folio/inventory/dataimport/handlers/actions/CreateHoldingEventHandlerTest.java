package org.folio.inventory.dataimport.handlers.actions;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.services.OrderHelperServiceImpl;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class CreateHoldingEventHandlerTest {

  private static final String PARSED_CONTENT_WITH_INSTANCE_ID = "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_CONTENT_WITHOUT_INSTANCE_ID = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
  private static final String FOLIO_SOURCE_ID = "f32d531e-df79-46b3-8932-cdd35f7a2264";
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String ITEM_ID = UUID.randomUUID().toString();
  private static final String ERRORS = "ERRORS";
  private static Reader fakeReader;
  private static final String permanentLocationId = UUID.randomUUID().toString();

  @Mock
  private Storage storage;
  @Mock
  private HoldingsRecordCollection holdingsRecordsCollection;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private IdStorageService holdingsIdStorageService;
  @Mock
  private OrderHelperServiceImpl orderHelperService;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(HOLDINGS);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withName("permanentLocationId").withPath("permanentLocationId").withValue("permanentLocationExpression").withEnabled("true"))));

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

  private CreateHoldingEventHandler createHoldingEventHandler;

  @Before
  public void setUp() throws UnsupportedEncodingException {
    MockitoAnnotations.initMocks(this);
    MappingManager.clearReaderFactories();
    createHoldingEventHandler = new CreateHoldingEventHandler(storage, mappingMetadataCache, holdingsIdStorageService, orderHelperService);
    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      RecordToEntity recordToItem = RecordToEntity.builder().recordId(RECORD_ID).entityId(ITEM_ID).build();
      return Future.succeededFuture(recordToItem);
    }).when(holdingsIdStorageService).store(any(), any(), any());

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    when(orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(any(), any(), any())).thenReturn(Future.succeededFuture());
    fakeReader = Mockito.mock(Reader.class);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(ListValue.of(List.of(permanentLocationId)));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "5", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonObject holding = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getJsonObject(0);
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(0, errors.size());
    Assert.assertNotNull(holding.getString("id"));
    Assert.assertEquals(instanceId, holding.getString("instanceId"));
    Assert.assertEquals(permanentLocationId, holding.getString("permanentLocationId"));
    Assert.assertEquals(FOLIO_SOURCE_ID, holding.getString("sourceId"));
  }

  @Test
  public void shouldProcessEventAndReturnErrorsInContext() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String testError = "testError";
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(testError, 400));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(), any());


    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "5", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdings = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(0, holdings.size());
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(testError, errors.getString(0));
  }

  @Test
  public void shouldProcessEventAndCreateMultipleHoldings() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    List<String> locations = List.of(permanentLocationId, UUID.randomUUID().toString());
    when(fakeReader.read(any(MappingRule.class))).thenReturn(ListValue.of(locations));
    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "5", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(2, holdingsList.size());

    for (int i = 0; i < holdingsList.size(); i++) {
      JsonObject holdingsAsJson = holdingsList.getJsonObject(i);
      Assert.assertNotNull(holdingsAsJson.getString("id"));
      Assert.assertEquals(instanceId, holdingsAsJson.getString("instanceId"));
      Assert.assertEquals(locations.get(i), holdingsAsJson.getString("permanentLocationId"));
      Assert.assertEquals(FOLIO_SOURCE_ID, holdingsAsJson.getString("sourceId"));
    }
  }

  @Test
  public void shouldProcessEventAndCreateMultipleHoldingsAndPopulateErrorMessagesForFailedHoldings() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String permanentLocationId2 = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(ListValue.of(List.of(permanentLocationId, permanentLocationId2)));

    String testError = "testError";

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(testError, 400));
      return null;
    }).when(holdingsRecordsCollection).add(argThat(holdingsRecord -> holdingsRecord.getPermanentLocationId().equals(permanentLocationId2)), any(), any());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "5", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray holdingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(1, holdingsList.size());
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(testError, errors.getString(0));

    holdingsList.forEach(holdings -> {
      JsonObject holdingsAsJson = (JsonObject) holdings;
      Assert.assertNotNull(holdingsAsJson.getString("id"));
      Assert.assertEquals(instanceId, holdingsAsJson.getString("instanceId"));
      Assert.assertEquals(permanentLocationId, holdingsAsJson.getString("permanentLocationId"));
      Assert.assertEquals(FOLIO_SOURCE_ID, holdingsAsJson.getString("sourceId"));
    });
  }

  @Test
  public void shouldProcessEventIfInstanceIdIsNotExistsInInstanceInContextButExistsInMarcBibliographicParsedRecords() throws InterruptedException, ExecutionException, TimeoutException {
    String expectedInstanceId = UUID.randomUUID().toString();
    JsonObject instanceAsJson = new JsonObject().put("id", expectedInstanceId);

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));

    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", instanceAsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonObject holding = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getJsonObject(0);
    Assert.assertNotNull(holding.getString("id"));
    Assert.assertEquals(expectedInstanceId, holding.getString("instanceId"));
    Assert.assertEquals(permanentLocationId, holding.getString("permanentLocationId"));
    Assert.assertEquals(FOLIO_SOURCE_ID, holding.getString("sourceId"));
  }

  @Test
  public void shouldProcessEventIfInstanceIdIsEmptyInInstanceInContextButExistsInMarcBibliographicParsedRecords() throws InterruptedException, ExecutionException, TimeoutException {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonObject holding = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value())).getJsonObject(0);
    Assert.assertNotNull(holding.getString("id"));
    Assert.assertEquals(permanentLocationId, holding.getString("permanentLocationId"));
    Assert.assertEquals(FOLIO_SOURCE_ID, holding.getString("sourceId"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventEvenIfDuplicatedInventoryStorageErrorExists() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(), any());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "5", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfInstanceIdIsNotExistsInInstanceInContextAndNotExistsInParsedRecords() throws InterruptedException, ExecutionException, TimeoutException  {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITHOUT_INSTANCE_ID));

    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject().encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfInstanceIdIsNotExistsInInstanceInContextAndMarcBibliographicNotExists() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceId = String.valueOf(UUID.randomUUID());
    Record record = new Record().withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject().encode());
    context.put("InvalidField", JsonObject.mapFrom(record).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfNoContextMarcBibliographic() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "9", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfPermanentLocationIdIsNotExistsInContext() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(fakeReader.read(any(MappingRule.class))).thenReturn(ListValue.of(List.of("")));

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "8", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfHoldingRecordIsInvalid() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Prelim item from MARC")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
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

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(UUID.randomUUID().toString()), StringValue.of(UUID.randomUUID().toString()));

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "7", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventWhenRecordToHoldingsFutureFails() throws ExecutionException, InterruptedException, TimeoutException, UnsupportedEncodingException {
    when(holdingsIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Something wrong with database!")));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingAsJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())
        .withContentType(ACTION_PROFILE));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(createHoldingEventHandler.isEligible(dataImportEventPayload));
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
    assertFalse(createHoldingEventHandler.isEligible(dataImportEventPayload));
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
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(createHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotHoldings() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(createHoldingEventHandler.isEligible(dataImportEventPayload));
  }
}
