package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
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
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.ItemsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
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

import static java.lang.String.format;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
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
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  private static final String PARSED_CONTENT_WITH_HOLDING_ID = "{\"leader\":\"01314nam 22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"AM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"h\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } }]}";
  private static final String MULTIPLE_HOLDINGS_FIELD = "MULTIPLE_HOLDINGS_FIELD";
  private static final String HOLDINGS_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
  private static final String ERRORS = "ERRORS";
  private static final String PERMANENT_LOCATION_ID = "fe19bae4-da28-472b-be90-d442e2428ead";

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
    }).when(mockedHoldingsCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    MappingManager.clearReaderFactories();
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerMapperFactory(new ItemsMapperFactory());
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

    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), itemsList.encode());

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

    JsonArray updatedItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(firstExistingItemJson.getString("id"), updatedItems.getJsonObject(0).getString("id"));
    Assert.assertEquals(firstExistingItemJson.getString(HRID_KEY), updatedItems.getJsonObject(0).getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(firstExistingItemJson, "permanentLoanType", "id"), updatedItems.getJsonObject(0).getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(firstExistingItemJson, "materialType", "id"), updatedItems.getJsonObject(0).getString("materialTypeId"));
    Assert.assertEquals(firstExistingItemJson.getString("holdingsRecordId"), updatedItems.getJsonObject(0).getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), updatedItems.getJsonObject(0).getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
  }

  @Test
  public void shouldUpdateMultipleItemsWithNewStatuses()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialTypeId", UUID.randomUUID().toString())
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));

    String permanentLocationId2 = UUID.randomUUID().toString();

    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(ITEM.value(), itemsList.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

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

    JsonArray updatedItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(2, updatedItems.size());
    JsonObject firstItem = updatedItems.getJsonObject(0);
    Assert.assertEquals(firstExistingItemJson.getString("id"), firstItem.getString("id"));
    Assert.assertEquals(firstExistingItemJson.getString(HRID_KEY), firstItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(firstExistingItemJson, "permanentLoanType", "id"), firstItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(firstExistingItemJson.getString("materialTypeId"), firstItem.getString("materialTypeId"));
    Assert.assertEquals(firstExistingItemJson.getString("holdingsRecordId"), firstItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), firstItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));

    JsonObject secondItem = updatedItems.getJsonObject(1);
    Assert.assertEquals(secondExistingItemJson.getString("id"), secondItem.getString("id"));
    Assert.assertEquals(secondExistingItemJson.getString(HRID_KEY), secondItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "permanentLoanType", "id"), secondItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "materialType", "id"), secondItem.getString("materialTypeId"));
    Assert.assertEquals(secondExistingItemJson.getString("holdingsRecordId"), secondItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), secondItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
  }

  @Test
  public void shouldUpdateMultipleItemsWithNewStatusesIfHoldingAlreadyInPayload()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    String holdingsId = UUID.randomUUID().toString();
    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", holdingsId);

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", holdingsId);

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));

    String permanentLocationId2 = UUID.randomUUID().toString();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(HOLDINGS.value(), new JsonArray().add(new JsonObject().put("id", holdingsId)).encode());
    payloadContext.put(ITEM.value(), itemsList.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

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

    JsonArray updatedItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(2, updatedItems.size());
    JsonObject firstItem = updatedItems.getJsonObject(0);
    Assert.assertEquals(firstExistingItemJson.getString("id"), firstItem.getString("id"));
    Assert.assertEquals(firstExistingItemJson.getString(HRID_KEY), firstItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(firstExistingItemJson, "permanentLoanType", "id"), firstItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(firstExistingItemJson, "materialType", "id"), firstItem.getString("materialTypeId"));
    Assert.assertEquals(firstExistingItemJson.getString("holdingsRecordId"), firstItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), firstItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));

    JsonObject secondItem = updatedItems.getJsonObject(1);
    Assert.assertEquals(secondExistingItemJson.getString("id"), secondItem.getString("id"));
    Assert.assertEquals(secondExistingItemJson.getString(HRID_KEY), secondItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "permanentLoanType", "id"), secondItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "materialType", "id"), secondItem.getString("materialTypeId"));
    Assert.assertEquals(secondExistingItemJson.getString("holdingsRecordId"), secondItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), secondItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
  }


  @Test
  public void shouldUpdateMultipleItemsOnOLRetryAndRemoveRetryCounterFromPayloadViaSeveralRuns() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {

    // 10 ids for the Items
    List<String> itemIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      itemIds.add(UUID.randomUUID().toString());
    }

    // 10 holdingsIds for Items
    List<String> holdingsIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      holdingsIds.add(UUID.randomUUID().toString());
    }

    String commonId = UUID.randomUUID().toString();

    JsonObject metadata = new JsonObject();

    //actual Items which will returned as "actual" after optimistic locking errors
    Item actualItem = new Item(itemIds.get(0), "2", holdingsIds.get(0), "test", new Status(AVAILABLE), commonId, commonId, metadata);

    Item actualItem2 = new Item(itemIds.get(3), "2", holdingsIds.get(3), "test", new Status(AVAILABLE), commonId, commonId, metadata);

    Item actualItem3 = new Item(itemIds.get(4), "2", holdingsIds.get(4), "test", new Status(AVAILABLE), commonId, commonId, metadata);

    Item actualItem4 = new Item(itemIds.get(5), "2", holdingsIds.get(5), "test", new Status(AVAILABLE), commonId, commonId, metadata);

    //Real Items which will have specific behavior: successful, optimistic locking error (ol), failure by another reason.
    JsonObject olItem1 = new JsonObject()
      .put("id", itemIds.get(0))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(0));
    JsonObject successfulItem2 = new JsonObject()
      .put("id", itemIds.get(1))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(1));
    JsonObject partialErrorItem3 = new JsonObject()
      .put("id", itemIds.get(2))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(2));
    JsonObject olItem4 = new JsonObject()
      .put("id", itemIds.get(3))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(3));
    JsonObject olItem5 = new JsonObject()
      .put("id", itemIds.get(4))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(4));
    JsonObject olItem6 = new JsonObject()
      .put("id", itemIds.get(5))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(5));
    JsonObject successfulItem7 = new JsonObject()
      .put("id", itemIds.get(6))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(6));
    JsonObject successfulItem8 = new JsonObject()
      .put("id", itemIds.get(7))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(7));
    JsonObject partialErrorItem9 = new JsonObject()
      .put("id", itemIds.get(8))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(8));
    JsonObject partialErrorItem10 = new JsonObject()
      .put("id", itemIds.get(9))
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingsIds.get(9));

    JsonArray itemList = new JsonArray();
    itemList.add(new JsonObject().put("item", olItem1));
    itemList.add(new JsonObject().put("item", successfulItem2));
    itemList.add(new JsonObject().put("item", partialErrorItem3));
    itemList.add(new JsonObject().put("item", olItem4));
    itemList.add(new JsonObject().put("item", olItem5));
    itemList.add(new JsonObject().put("item", olItem6));
    itemList.add(new JsonObject().put("item", successfulItem7));
    itemList.add(new JsonObject().put("item", successfulItem8));
    itemList.add(new JsonObject().put("item", partialErrorItem9));
    itemList.add(new JsonObject().put("item", partialErrorItem10));

    // Provide behavior for the items
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(0)), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
        Item itemRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(itemRecord));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", itemIds.get(2)), 404));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(3)), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(4)), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(5)), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Item itemRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(itemRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Item itemRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(itemRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", itemIds.get(8)), 404));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", itemIds.get(9)), 404));
        return null;
      })// 10 Items processed. Next iteration will be on the second run 'handle()'-method.
      .doAnswer(invocationOnMock -> {
        Item itemRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(itemRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(3)), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", itemIds.get(4)), 404));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Item itemRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(itemRecord));
        return null;
      }).when(mockedItemCollection).update(any(), any(), any());


    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.contains("AND id <>")),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(actualItem, actualItem2, actualItem3, actualItem4), 4);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s OR %s OR %s)", itemIds.get(0), itemIds.get(3), itemIds.get(4), itemIds.get(5)))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), itemList.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(mockedItemCollection, times(14)).update(any(), any(), any());
    verify(mockedItemCollection, times(1)).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s OR %s OR %s)", itemIds.get(0), itemIds.get(3), itemIds.get(4), itemIds.get(5)))), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ITEM.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(UpdateHoldingEventHandler.CURRENT_RETRY_NUMBER));
    JsonArray resultedItems = new JsonArray(actualDataImportEventPayload.getContext().get(ITEM.value()));

    Assert.assertEquals(5, resultedItems.size());
    assertEquals(itemIds.get(1), String.valueOf(resultedItems.getJsonObject(0).getString("id")));
    assertEquals(itemIds.get(6), String.valueOf(resultedItems.getJsonObject(1).getString("id")));
    assertEquals(itemIds.get(7), String.valueOf(resultedItems.getJsonObject(2).getString("id")));
    assertEquals(itemIds.get(0), String.valueOf(resultedItems.getJsonObject(3).getString("id")));
    assertEquals(itemIds.get(5), String.valueOf(resultedItems.getJsonObject(4).getString("id")));

    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ERRORS));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(5, resultedErrorList.size());
    assertEquals(itemIds.get(2), String.valueOf(resultedErrorList.get(0).getId()));
    assertEquals(itemIds.get(8), String.valueOf(resultedErrorList.get(1).getId()));
    assertEquals(itemIds.get(9), String.valueOf(resultedErrorList.get(2).getId()));
    assertEquals(itemIds.get(4), String.valueOf(resultedErrorList.get(3).getId()));
    assertEquals(itemIds.get(3), String.valueOf(resultedErrorList.get(4).getId()));
    assertEquals(resultedErrorList.get(0).getError(), format("Cannot update record %s not found", itemIds.get(2)));
    assertEquals(resultedErrorList.get(1).getError(), format("Cannot update record %s not found", itemIds.get(8)));
    assertEquals(resultedErrorList.get(2).getError(), format("Cannot update record %s not found", itemIds.get(9)));
    assertEquals(resultedErrorList.get(3).getError(), format("Cannot update record %s not found", itemIds.get(4)));
    assertEquals(resultedErrorList.get(4).getError(), format("Current retry number %s exceeded or equal given number %s for the Item update for jobExecutionId '%s' ", 1, 1, actualDataImportEventPayload.getJobExecutionId()));


    //Second run. We need it to verify that CURRENT_RETRY_NUMBER and all accumulative results are cleared.
    JsonArray itemsListSecondRun = new JsonArray();
    itemsListSecondRun.add(new JsonObject().put("item", olItem1));
    itemsListSecondRun.add(new JsonObject().put("item", successfulItem2));
    itemsListSecondRun.add(new JsonObject().put("item", partialErrorItem3));
    itemsListSecondRun.add(new JsonObject().put("item", olItem4));

    HashMap<String, String> contextSecondRun = new HashMap<>();
    contextSecondRun.put(ActionProfile.FolioRecord.ITEM.value(), Json.encode(itemsListSecondRun));
    contextSecondRun.put(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayloadSecondRun = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(contextSecondRun)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));


    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(0)), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
        Item tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", itemIds.get(2)), 404));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemIds.get(3)), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Item tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Item tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .when(mockedItemCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(List.of(actualItem, actualItem2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", itemIds.get(0), itemIds.get(3)))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    CompletableFuture<DataImportEventPayload> futureSecondRun = updateItemHandler.handle(dataImportEventPayloadSecondRun);
    DataImportEventPayload actualDataImportEventPayloadSecondRun = futureSecondRun.get(10000, TimeUnit.MILLISECONDS);
    verify(mockedItemCollection, times(20)).update(any(), any(), any());
    verify(mockedItemCollection, times(1)).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", itemIds.get(0), itemIds.get(3)))), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED.value(), actualDataImportEventPayloadSecondRun.getEventType());
    Assert.assertNotNull(actualDataImportEventPayloadSecondRun.getContext().get(ITEM.value()));
    Assert.assertNull(actualDataImportEventPayloadSecondRun.getContext().get(UpdateHoldingEventHandler.CURRENT_RETRY_NUMBER));

    JsonArray resultedItemsRecordsSecondRun = new JsonArray(actualDataImportEventPayloadSecondRun.getContext().get(ITEM.value()));
    Assert.assertEquals(3, resultedItemsRecordsSecondRun.size());

    assertEquals(successfulItem2.getString("id"), String.valueOf(resultedItemsRecordsSecondRun.getJsonObject(0).getString("id")));
    assertEquals(olItem1.getString("id"), String.valueOf(resultedItemsRecordsSecondRun.getJsonObject(1).getString("id")));
    assertEquals(olItem4.getString("id"), String.valueOf(resultedItemsRecordsSecondRun.getJsonObject(2).getString("id")));

    Assert.assertNotNull(actualDataImportEventPayloadSecondRun.getContext().get(ERRORS));
    List<PartialError> resultedErrorListSecondRun = List.of(Json.decodeValue(actualDataImportEventPayloadSecondRun.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(1, resultedErrorListSecondRun.size());
    assertEquals(partialErrorItem3.getString("id"), String.valueOf(resultedErrorListSecondRun.get(0).getId()));
    assertEquals(resultedErrorListSecondRun.get(0).getError(), format("Cannot update record %s not found", partialErrorItem3.getString("id")));
  }

  @Test
  public void shouldNotUpdateSingleItemIfOLErrorExistsAndRetryNumberIsExceeded() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    String itemId = UUID.randomUUID().toString();

    String holdingId = String.valueOf(UUID.randomUUID());

    String commonId = UUID.randomUUID().toString();

    JsonObject metadata = new JsonObject();

    Item actualItem = new Item(itemId, "2", holdingId, "test", new Status(AVAILABLE), commonId, commonId, metadata);

    JsonObject olItem1 = new JsonObject()
      .put("id", itemId)
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingId);

    JsonArray itemList = new JsonArray();
    itemList.add(new JsonObject().put("item", olItem1));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), itemList.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemId), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemId), 409));
      return null;
    }).when(mockedItemCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.contains("AND id <>")),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(List.of(actualItem), 1);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", itemId))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(mockedItemCollection, times(2)).update(any(), any(), any());
    verify(mockedItemCollection, times(1)).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", itemId))), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ActionProfile.FolioRecord.ITEM.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(UpdateHoldingEventHandler.CURRENT_RETRY_NUMBER));


    JsonArray resultedItemsRecordsSecondRun = new JsonArray(actualDataImportEventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(0, resultedItemsRecordsSecondRun.size());

    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ERRORS));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(1, resultedErrorList.size());
    assertEquals(itemId, String.valueOf(resultedErrorList.get(0).getId()));
    assertEquals(format("Current retry number 1 exceeded or equal given number 1 for the Item update for jobExecutionId '%s' ", actualDataImportEventPayload.getJobExecutionId()), resultedErrorList.get(0).getError());
  }


  @Test
  public void shouldUpdateSingleItemEvenIfOLErrorExistsAndRemoveRetryCounterFromPayload() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    String itemId = UUID.randomUUID().toString();

    String holdingId = String.valueOf(UUID.randomUUID());

    String commonId = UUID.randomUUID().toString();

    JsonObject metadata = new JsonObject();

    Item actualItem = new Item(itemId, "2", holdingId, "test", new Status(AVAILABLE), commonId, commonId, metadata);

    JsonObject olItem1 = new JsonObject()
      .put("id", itemId)
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", commonId))
      .put("permanentLoanType", new JsonObject().put("id", commonId))
      .put("holdingsRecordId", holdingId);

    JsonArray itemList = new JsonArray();
    itemList.add(new JsonObject().put("item", olItem1));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), itemList.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", itemId), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
      Item tmpHoldingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(tmpHoldingsRecord));
      return null;
    }).when(mockedItemCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.contains("AND id <>")),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(List.of(actualItem), 1);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", itemId))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(mockedItemCollection, times(2)).update(any(), any(), any());
    verify(mockedItemCollection, times(1)).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", itemId))), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ActionProfile.FolioRecord.ITEM.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(UpdateHoldingEventHandler.CURRENT_RETRY_NUMBER));

    JsonArray resultedItems = new JsonArray(actualDataImportEventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(1, resultedItems.size());
    assertEquals(itemId, String.valueOf(resultedItems.getJsonObject(0).getString("id")));

    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ERRORS));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(0, resultedErrorList.size());
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotUpdateMultipleItemsWithNewStatusesAndReturnDiErrorIfNoItemsUpdated()
    throws UnsupportedEncodingException, ExecutionException, InterruptedException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingRule statusMappingRule = new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true");
    when(fakeReader.read(eq(statusMappingRule))).thenReturn(StringValue.of("Invalid status"));

    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));

    String permanentLocationId2 = UUID.randomUUID().toString();

    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(ITEM.value(), itemsList.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

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
  public void shouldAddPartialErrorsWhenBarcodeToUpdatedAssignedToAnotherItem()
    throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    // given
    String itemId1 = UUID.randomUUID().toString();
    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", itemId1)
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(argThat(cql -> cql.contains(itemId1)),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));

    String permanentLocationId2 = UUID.randomUUID().toString();

    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(ITEM.value(), itemsList.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

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

    JsonArray updatedItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(1, updatedItems.size());
    JsonObject firstItem = updatedItems.getJsonObject(0);
    Assert.assertEquals(secondExistingItemJson.getString("id"), firstItem.getString("id"));
    Assert.assertEquals(secondExistingItemJson.getString(HRID_KEY), firstItem.getString(HRID_KEY));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "permanentLoanType", "id"), firstItem.getString("permanentLoanTypeId"));
    Assert.assertEquals(getNestedProperty(secondExistingItemJson, "materialType", "id"), firstItem.getString("materialTypeId"));
    Assert.assertEquals(secondExistingItemJson.getString("holdingsRecordId"), firstItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), firstItem.getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertNotNull(eventPayload.getContext().get(HOLDINGS.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));

    JsonObject firstError = errors.getJsonObject(0);
    Assert.assertEquals(firstExistingItemJson.getString("id"), firstError.getString("id"));
    Assert.assertTrue(firstError.getString("error").contains("Barcode must be unique, In process is already assigned to another item"));
  }

  @Test
  public void shouldNotRequestWhenUpdatedItemHasEmptyBarcode()
    throws UnsupportedEncodingException, ExecutionException, InterruptedException, TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    String permanentLocationId2 = UUID.randomUUID().toString();

    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", expectedHoldingId1);

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", expectedHoldingId2);

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));


    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(ITEM.value(), itemsList.encode());

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
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
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

    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED, DataImportEventTypes.fromValue(eventPayload.getEventType()));
    Assert.assertEquals(2, new JsonArray(eventPayload.getContext().get(HOLDINGS.value())).size());
    Assert.assertEquals(2, new JsonArray(eventPayload.getContext().get(ITEM.value())).size());

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
    throws UnsupportedEncodingException, ExecutionException, InterruptedException, TimeoutException {
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

    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    firstExistingItemJson.put(STATUS_KEY, new JsonObject().put("name", protectedItemStatus));
    secondExistingItemJson.put(STATUS_KEY, new JsonObject().put("name", protectedItemStatus));


    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));

    String permanentLocationId2 = UUID.randomUUID().toString();

    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(ITEM.value(), itemsList.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    // then
    JsonArray updatedItem = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(protectedItemStatus, updatedItem.getJsonObject(0).getJsonObject(STATUS_KEY).getString("name"));
    Assert.assertEquals(expectedItemBarcode, updatedItem.getJsonObject(0).getString(ItemUtil.BARCODE));

    Assert.assertEquals(2, new JsonArray(eventPayload.getContext().get(ITEM.value())).size());
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));

    JsonObject firstError = errors.getJsonObject(0);
    Assert.assertEquals(firstExistingItemJson.getString("id"), firstError.getString("id"));
    Assert.assertTrue(firstError.getString("error").contains("Could not change item status"));

    JsonObject secondError = errors.getJsonObject(1);
    Assert.assertEquals(secondExistingItemJson.getString("id"), secondError.getString("id"));
    Assert.assertTrue(secondError.getString("error").contains("Could not change item status"));
  }

  @Test
  public void shouldReturnFailedFutureWhenCurrentActionProfileHasNoMappingProfile() {
    // given
    JsonObject firstExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonObject secondExistingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    JsonArray itemsList = new JsonArray();
    itemsList.add(new JsonObject().put("item", firstExistingItemJson));
    itemsList.add(new JsonObject().put("item", secondExistingItemJson));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), itemsList.encode());

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
