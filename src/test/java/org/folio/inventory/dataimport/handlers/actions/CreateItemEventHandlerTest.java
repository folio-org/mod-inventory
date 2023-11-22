package org.folio.inventory.dataimport.handlers.actions;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.dataimport.ItemsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
import org.folio.inventory.dataimport.services.OrderHelperServiceImpl;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateItemEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateItemEventHandlerTest {
  private static final String PARSED_CONTENT_WITHOUT_HOLDING_ID = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";
  private static final String PARSED_CONTENT_WITH_HOLDING_ID = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"AM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"h\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } }]}";
  private static final String PARSED_CONTENT_WITH_INVALID_MULTIPLE_FIELDS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"subfields\":[{\"a\":\"AM\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"945\":{\"subfields\":[{\"a\":\"OM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"945\":{\"subfields\":[{\"a\":\"AM\"},{\"h\":\"KU/CC/DI/M\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"945\":{\"subfields\":[{\"h\":\"fake\"}],\"ind1\":\" \",\"ind2\":\" \"}}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"h\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } }]}";
  private static final String ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE = "All Items should have the same material type, during the creation of open order";
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String ITEM_ID = UUID.randomUUID().toString();
  private static final String PERMANENT_LOCATION_ID = "ff4524ee-89b2-461d-82d6-2b4127b801f9";
  private static final String ERRORS = "ERRORS";
  private static final String MULTIPLE_HOLDINGS_FIELD = "MULTIPLE_HOLDINGS_FIELD";
  private static final String HOLDINGS_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
  private static final String EMPTY_JSON_ARRAY = "[]";

  @Mock
  private Storage mockedStorage;
  @Mock
  private ItemCollection mockedItemCollection;
  @Mock
  private Reader fakeReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private IdStorageService itemIdStorageService;
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
    .withFolioRecord(ActionProfile.FolioRecord.ITEM);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(ITEM)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Arrays.asList(
        new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"").withEnabled("true"),
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

  private CreateItemEventHandler createItemHandler;

  @Before
  public void setUp() throws UnsupportedEncodingException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()), StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()), StringValue.of("645398607547"));
    Mockito.when(mockedStorage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedItemCollection);

    Mockito.when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    RecordToEntity recordToItem = RecordToEntity.builder().recordId(RECORD_ID).entityId(ITEM_ID).build();
    when(itemIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToItem));

    Mockito.doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    createItemHandler = new CreateItemEventHandler(mockedStorage, mappingMetadataCache, itemIdStorageService, orderHelperService);
    MappingManager.clearReaderFactories();
    when(orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(any(), any(), any())).thenReturn(Future.succeededFuture());
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerMapperFactory(new ItemsMapperFactory());
  }

  @Test
  public void shouldCreateItemAndFillInHoldingsRecordIdFromHoldingsEntityAndFillInPurchaseOrderLineIdentifierFromPoLineEntity()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID())
        .put("permanentLocationId", UUID.randomUUID()),
      new JsonObject()
        .put("id", expectedHoldingId)
        .put("permanentLocationId", PERMANENT_LOCATION_ID)));
    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));
    JsonObject createdItem = createdItems.getJsonObject(0);
    Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
    Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
    Assert.assertNotNull(createdItem.getString("materialTypeId"));
    Assert.assertEquals(expectedHoldingId, createdItem.getString("holdingId"));
    Assert.assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
  }

  @Test
  public void shouldCreateMultipleItems()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String materialTypeId = UUID.randomUUID().toString();
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"));
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
    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
      Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
      Assert.assertNotNull(createdItem.getString("materialTypeId"));
      Assert.assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
      Assert.assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @Test
  public void shouldCreateMultipleItemsAndSkipItemsWithInvalidHoldingsIdentifiers()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String materialTypeId = UUID.randomUUID().toString();
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"));
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
    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INVALID_MULTIPLE_FIELDS));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(Lists.newArrayList(null, PERMANENT_LOCATION_ID, permanentLocationId2, "fake")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(2, createdItems.size());
    Assert.assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    Assert.assertEquals(ITEM_ID, createdItems.getJsonObject(0).getString("id"));
    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
      Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
      Assert.assertNotNull(createdItem.getString("materialTypeId"));
      Assert.assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
      Assert.assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @Test
  public void shouldCreateMultipleItemsAndPopulatePartialErrorsForFailedItems()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    String testError = "testError";
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    Mockito.doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(testError, 400));
      return null;
    }).when(mockedItemCollection).add(argThat(itemRecord -> itemRecord.getHoldingId().equals(expectedHoldingId1)), any(), any());

    String materialTypeId = UUID.randomUUID().toString();
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));
    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(ITEM_ID, errors.getJsonObject(0).getString("id"));
    Assert.assertEquals(errors.getJsonObject(0).getString("error"), testError);
    Assert.assertEquals(errors.getJsonObject(0).getString("holdingId"), expectedHoldingId1);

    JsonObject createdItem = createdItems.getJsonObject(0);
    Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
    Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
    Assert.assertNotNull(createdItem.getString("materialTypeId"));
    Assert.assertEquals(holdingsAsJson.getJsonObject(1).getString("id"), createdItem.getString("holdingId"));
    Assert.assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
  }

  @Test
  public void shouldPopulateSameHoldingsItForAllItemsIfOnlyOneHoldingExist()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String materialTypeId = UUID.randomUUID().toString();
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"));

    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID)));

    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
      Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
      Assert.assertNotNull(createdItem.getString("materialTypeId"));
      Assert.assertEquals(expectedHoldingId1, createdItem.getString("holdingId"));
      Assert.assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @Test
  public void shouldCreateItemAndFillInHoldingsRecordIdFromParsedRecordContent()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));
    payloadContext.put(ERRORS, Json.encode(new PartialError(null, "testError")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(1, createdItems.size());
    JsonObject createdItem = createdItems.getJsonObject(0);
    Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
    Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
    Assert.assertNotNull(createdItem.getString("materialTypeId"));
    Assert.assertEquals(createdItem.getString("holdingId"), expectedHoldingId);
  }

  @Test
  public void shouldCreateItemAndFillInHoldingsRecordIdFromMatchedHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String permanentLocationId = UUID.randomUUID().toString();
    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId).put("permanentLocationId", permanentLocationId);
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(ERRORS, Json.encode(new PartialError(null, "testError")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(1, createdItems.size());
    JsonObject createdItem = createdItems.getJsonObject(0);
    Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
    Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
    Assert.assertNotNull(createdItem.getString("materialTypeId"));
    Assert.assertEquals(createdItem.getString("holdingId"), expectedHoldingId);
  }

  @Test
  public void shouldNotReturnFailedFutureIfInventoryStorageErrorExists()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    String errorMsg = "Smth error";

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(errorMsg, 400));
      return null;
    }).when(mockedItemCollection).add(argThat(item -> item.getBarcode().equals("745398607547")), any(), any());

    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(argThat(item -> item.getBarcode().equals("645398607547")), any(Consumer.class), any(Consumer.class));

    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));

    HashMap<String, String> payloadContext = new HashMap<>();
    String permanentLocationId2 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(errorMsg, errors.getJsonObject(0).getString("error"));
    Assert.assertEquals(ITEM_ID, errors.getJsonObject(0).getString("id"));
  }

  @Test
  public void shouldCompleteFutureAndReturnErrorsWhenMappedItemWithoutStatus()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    // given
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(""),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void shouldCompleteAndReturnErrorWhenMappedItemWithUnrecognizedStatusName()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    // given
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of("fakeStatus"),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void shouldCompleteAndReturnErrorWhenCreatedItemHasExistingBarcode()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    Mockito.doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(argThat(query -> query.contains("745398607547")), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void shouldCompleteReturnErrorWhenMappedItemWithoutPermanentLoanType()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));
    // given
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(null),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, createdItems.size());
    Assert.assertEquals(1, errors.size());
  }


  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureIfDuplicatedErrorExists()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    // given
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    // given
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldNotRequestWhenCreatedItemHasEmptyBarcode()
    throws UnsupportedEncodingException, ExecutionException, InterruptedException, TimeoutException {

    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(argThat(query -> query.contains("745398607547")), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));
    String permanentLocationId2 = UUID.randomUUID().toString();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(holdingsAsJson));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

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
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    // then
    verify(mockedItemCollection, Mockito.times(0))
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenCouldNotFindHoldingsRecordIdInEventPayload()
    throws
    InterruptedException,
    ExecutionException,
    TimeoutException {
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITHOUT_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenCouldNotFindPoLineIdInEventPayload()
    throws
    InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));
    payloadContext.put(EntityType.PO_LINE.value(), new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldFailWhenNoItemsCreated()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()), StringValue.of(""), StringValue.of(UUID.randomUUID().toString()));

    JsonObject holdingAsJson = new JsonObject().put("id", UUID.randomUUID().toString());
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void shouldReturnFailedFutureWhenCurrentActionProfileHasNoMappingProfile() {
    // given
    CreateItemEventHandler createItemHandler = new CreateItemEventHandler(mockedStorage, mappingMetadataCache, itemIdStorageService, orderHelperService);
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withContentType(ACTION_PROFILE));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventWhenRecordToItemFutureFails() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    when(itemIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Something wrong with database!")));

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

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnFailedFutureWhenTryingToCreateItemsWithDifferentMaterialTypesDuringCreationOfOpenOrder() {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
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
    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE, exception.getCause().getMessage());
  }

  @Test
  public void shouldCreateMultipleItemsWithDifferentMaterialTypesWhenNoPoLineInTheContext()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
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
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    Assert.assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
      Assert.assertNotNull(createdItem.getString("permanentLoanTypeId"));
      Assert.assertNotNull(createdItem.getString("materialTypeId"));
      Assert.assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
    }
  }
}
