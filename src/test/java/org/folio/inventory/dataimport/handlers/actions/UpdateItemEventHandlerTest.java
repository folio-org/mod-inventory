package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.handlers.actions.UpdateItemEventHandler.CURRENT_RETRY_NUMBER;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.HoldingsRecord;
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.ItemUtil;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class UpdateItemEventHandlerTest {

  private static final String PARSED_CONTENT = "{{ \n" +
    "  \"fields\": [\n" +
    "    { \"900\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"it00000000001\" }, { \"3\": \"ho00000000001\" } ] } },\n" +
    "    { \"950\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"Did this item note get added?\" } ] } },\n" +
    "    { \"951\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"Did this item check-out note get added?\" } ] } },\n" +
    "    { \"999\": { \"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [ { \"s\": \"bd9894be-e5ea-424f-aa47-1dd54e719ed4\" }, { \"i\": \"45fd6cfe-5b7c-43f3-9fc3-bd80261b328f\" } ] } }],\n" +
    "  \"leader\": \"01877cam a2200457Ii 4500\"\n" +
    "}";

  @Mock
  private Storage mockedStorage;
  @Mock
  private ItemCollection mockedItemCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsCollection;
  @Mock
  private Reader fakeReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update item-SR")
    .withAction(UPDATE)
    .withFolioRecord(ActionProfile.FolioRecord.ITEM);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(ITEM)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Arrays.asList(
        new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.barcode").withValue("\"statusExpression\"").withEnabled("true")
      )));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private JsonObject existingItemJson;

  private UpdateItemEventHandler updateItemHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      return CompletableFuture.completedFuture(item);
    }).when(mockedItemCollection).update(any(Item.class));

    Item returnedItem = new Item(UUID.randomUUID().toString(), "2", UUID.randomUUID().toString(), "test", new Status(AVAILABLE),
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), new JsonObject());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedItem));
      return null;
    }).when(mockedItemCollection).update(any(), any(), any());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(IN_PROCESS.value()));
    when(mockedStorage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedItemCollection);
    when(mockedStorage.getHoldingsRecordCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedHoldingsCollection);


    String instanceId = String.valueOf(UUID.randomUUID());
    String holdingId = UUID.randomUUID().toString();
    String hrid = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();

    doAnswer(invocationOnMock -> {
      HoldingsRecord returnedHoldings = new HoldingsRecord()
        .withId(holdingId)
        .withHrid(hrid)
        .withInstanceId(instanceId)
        .withPermanentLocationId(permanentLocationId)
        .withVersion(1);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedHoldings));
      return null;
    }).when(mockedHoldingsCollection).findById(anyString(),any(Consumer.class),any(Consumer.class));

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    MappingManager.clearReaderFactories();
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    updateItemHandler = new UpdateItemEventHandler(mockedStorage, mappingMetadataCache);

    existingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());
  }

  @Test
  public void shouldUpdateItemWithNewStatus()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED, DataImportEventTypes.fromValue(eventPayload.getEventType()));
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonObject updatedItem = new JsonObject(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(existingItemJson.getString("id"), updatedItem.getString("id"));
    Assert.assertEquals(existingItemJson.getString(HRID_KEY), updatedItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(existingItemJson, "permanentLoanType", "id"), updatedItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(existingItemJson, "materialType", "id"), updatedItem.getString("materialTypeId"));
    Assert.assertEquals(existingItemJson.getString("holdingsRecordId"), updatedItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), updatedItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
  }

  @Test
  public void shouldUpdateItemWithNewStatusIfHoldingAlreadyInPayload()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());
    payloadContext.put(HOLDINGS.value(), "{notEmptyValue}");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED, DataImportEventTypes.fromValue(eventPayload.getEventType()));
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonObject updatedItem = new JsonObject(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(existingItemJson.getString("id"), updatedItem.getString("id"));
    Assert.assertEquals(existingItemJson.getString(HRID_KEY), updatedItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(existingItemJson, "permanentLoanType", "id"), updatedItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(existingItemJson, "materialType", "id"), updatedItem.getString("materialTypeId"));
    Assert.assertEquals(existingItemJson.getString("holdingsRecordId"), updatedItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), updatedItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
  }

  @Test
  public void shouldUpdateItemWithRetryAndRemoveRetryCounterFromPayload()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {

    String itemId = UUID.randomUUID().toString();
    String holdingId = UUID.randomUUID().toString();
    String materialTypeId = UUID.randomUUID().toString();
    String permanentLoanTypeId = UUID.randomUUID().toString();
    JsonObject metadata = new JsonObject();

    Item actualItem = new Item(itemId, "2", holdingId, "test", new Status(AVAILABLE), materialTypeId, permanentLoanTypeId, metadata);

    when(mockedItemCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(actualItem));

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("", 409));
      return null;
    }).doAnswer(invocationOnMock -> {
      Item itemRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(itemRecord));
      return null;
    }).when(mockedItemCollection).update(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    Item itemRecord = new Item(itemId, "2", holdingId, new Status(AVAILABLE), materialTypeId, permanentLoanTypeId, metadata);

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), ItemUtil.mapToMappingResultRepresentation(itemRecord));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);
    verify(mockedItemCollection, times(2)).update(any(), any(), any());
    verify(mockedItemCollection).findById(itemRecord.getId());

    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ITEM.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedWhenOLError()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(mockedItemCollection).update(any(), any(), any());

    Item returnedItem = new Item(existingItemJson.getString("id"), "2", UUID.randomUUID().toString(), "test", new Status(AVAILABLE),
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), new JsonObject());

    when(mockedItemCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(returnedItem));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithUnrecognizedStatusName()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    MappingRule statusMappingRule = new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true");
    when(fakeReader.read(eq(statusMappingRule))).thenReturn(StringValue.of("Invalid status"));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenBarcodeToUpdatedAssignedToAnotherItem()
    throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    // given
    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldNotRequestWhenUpdatedItemHasEmptyBarcode()
    throws UnsupportedEncodingException {
    // given
    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Prelim item from MARC")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(Arrays.asList(
          new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
          new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"").withEnabled("true"),
          new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"").withEnabled("true")
        )));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap()))));

    // when
    updateItemHandler.handle(dataImportEventPayload);

    // then
    verify(mockedItemCollection, Mockito.times(0))
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoExistingItemToUpdate()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  @Parameters({"Aged to lost", "Awaiting delivery", "Awaiting pickup", "Checked out", "Claimed returned", "Declared lost", "Paged"})
  public void shouldReturnFailedFutureAndUpdateItemExceptStatusWhenStatusCanNotBeUpdated(String protectedItemStatus)
    throws UnsupportedEncodingException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    String expectedItemBarcode = "BC-123123";
    MappingRule barcodeMappingRule = mappingProfile.getMappingDetails().getMappingFields().get(1);
    when(fakeReader.read(eq(barcodeMappingRule))).thenReturn(StringValue.of(expectedItemBarcode));

    existingItemJson.put(STATUS_KEY, new JsonObject().put("name", protectedItemStatus));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    Assert.assertTrue(future.isCompletedExceptionally());
    JsonObject updatedItem = new JsonObject(payloadContext.get(ITEM.value()));
    Assert.assertEquals(protectedItemStatus, updatedItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertEquals(expectedItemBarcode, updatedItem.getString(ItemUtil.BARCODE));
  }

  @Test
  public void shouldReturnFailedFutureWhenCurrentActionProfileHasNoMappingProfile() {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), existingItemJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withContentType(ACTION_PROFILE));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = updateItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.ITEM);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = updateItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }
}
