package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
import lombok.SneakyThrows;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
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
import org.folio.processing.value.ListValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CreateItemEventHandlerTest {

  private static final String PARSED_CONTENT_WITHOUT_HOLDING_ID = """
    {
      "leader": "01314nam  22003851a 4500",
      "fields": [
        {
          "001": "ybp7406411"
        }
      ]
    }
    """;
  private static final String PARSED_CONTENT_WITH_HOLDING_ID = """
    {
      "leader": "01314nam  22003851a 4500",
      "fields": [
        {
          "001": "ybp7406411"
        },
        {
          "945": {
            "subfields": [
              {
                "a": "OM"
              },
              {
                "h": "KU/CC/DI/M"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "945": {
            "subfields": [
              {
                "a": "AM"
              },
              {
                "h": "KU/CC/DI/M"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "999": {
            "ind1": "f",
            "ind2": "f",
            "subfields": [
              {
                "h": "957985c6-97e3-4038-b0e7-343ecd0b8120"
              }
            ]
          }
        }
      ]
    }
    """;
  private static final String PARSED_CONTENT_WITH_INVALID_MULTIPLE_FIELDS = """
    {
      "leader": "01314nam  22003851a 4500",
      "fields": [
        {
          "001": "ybp7406411"
        },
        {
          "945": {
            "subfields": [
              {
                "a": "AM"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "945": {
            "subfields": [
              {
                "a": "OM"
              },
              {
                "h": "KU/CC/DI/M"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "945": {
            "subfields": [
              {
                "a": "AM"
              },
              {
                "h": "KU/CC/DI/M"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "945": {
            "subfields": [
              {
                "h": "fake"
              }
            ],
            "ind1": " ",
            "ind2": " "
          }
        },
        {
          "999": {
            "ind1": "f",
            "ind2": "f",
            "subfields": [
              {
                "h": "957985c6-97e3-4038-b0e7-343ecd0b8120"
              }
            ]
          }
        }
      ]
    }
    """;
  private static final String ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE =
    "All Items should have the same material type, during the creation of open order";
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String ITEM_ID = UUID.randomUUID().toString();
  private static final String PERMANENT_LOCATION_ID = "ff4524ee-89b2-461d-82d6-2b4127b801f9";
  private static final String ERRORS = "ERRORS";
  private static final String MULTIPLE_HOLDINGS_FIELD = "MULTIPLE_HOLDINGS_FIELD";
  private static final String HOLDINGS_IDENTIFIERS = "HOLDINGS_IDENTIFIERS";
  private static final String EMPTY_JSON_ARRAY = "[]";

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.ITEM);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(ITEM)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Arrays.asList(
        new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.barcode").withValue("\"statusExpression\"").withEnabled("true")
      )));

  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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

  private CreateItemEventHandler createItemHandler;

  @BeforeEach
  void setUp() throws UnsupportedEncodingException {
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()), StringValue.of("645398607547"));
    when(mockedStorage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedItemCollection);

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    RecordToEntity recordToItem = RecordToEntity.builder().recordId(RECORD_ID).entityId(ITEM_ID).build();
    when(itemIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(recordToItem));

    doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(), any());

    createItemHandler =
      new CreateItemEventHandler(mockedStorage, mappingMetadataCache, itemIdStorageService, orderHelperService);
    MappingManager.clearReaderFactories();
    when(orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(any(), any(), any())).thenReturn(
      Future.succeededFuture());
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    MappingManager.registerMapperFactory(new ItemsMapperFactory());
  }

  @Test
  void shouldCreateItemAndFillInHoldingsRecordIdFromHoldingsEntityAndFillInPurchaseOrderLineIdentifierFromPoLineEntity()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

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
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(1, createdItems.size());
    assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));
    JsonObject createdItem = createdItems.getJsonObject(0);
    assertNotNull(createdItem.getJsonObject("status").getString("name"));
    assertNotNull(createdItem.getString("permanentLoanTypeId"));
    assertNotNull(createdItem.getString("materialTypeId"));
    assertEquals(expectedHoldingId, createdItem.getString("holdingId"));
    assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldCreateMultipleItems()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String materialTypeId = UUID.randomUUID().toString();
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));
    assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      assertNotNull(createdItem.getJsonObject("status").getString("name"));
      assertNotNull(createdItem.getString("permanentLoanTypeId"));
      assertNotNull(createdItem.getString("materialTypeId"));
      assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
      assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldCreateMultipleItemsAndSkipItemsWithInvalidHoldingsIdentifiers()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String materialTypeId = UUID.randomUUID().toString();
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INVALID_MULTIPLE_FIELDS));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS,
      Json.encode(Lists.newArrayList(null, PERMANENT_LOCATION_ID, permanentLocationId2, "fake")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(2, createdItems.size());
    assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    assertEquals(ITEM_ID, createdItems.getJsonObject(0).getString("id"));
    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      assertNotNull(createdItem.getJsonObject("status").getString("name"));
      assertNotNull(createdItem.getString("permanentLoanTypeId"));
      assertNotNull(createdItem.getString("materialTypeId"));
      assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
      assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldCreateMultipleItemsAndPopulatePartialErrorsForFailedItems()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    String expectedHoldingId2 = UUID.randomUUID().toString();
    String expectedHoldingId1 = UUID.randomUUID().toString();
    String testError = "testError";
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(testError, 400));
      return null;
    }).when(mockedItemCollection)
      .add(argThat(itemRecord -> itemRecord.getHoldingId().equals(expectedHoldingId1)), any(), any());

    String materialTypeId = UUID.randomUUID().toString();
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
    assertEquals(ITEM_ID, errors.getJsonObject(0).getString("id"));
    assertEquals(testError, errors.getJsonObject(0).getString("error"));
    assertEquals(errors.getJsonObject(0).getString("holdingId"), expectedHoldingId1);

    JsonObject createdItem = createdItems.getJsonObject(0);
    assertNotNull(createdItem.getJsonObject("status").getString("name"));
    assertNotNull(createdItem.getString("permanentLoanTypeId"));
    assertNotNull(createdItem.getString("materialTypeId"));
    assertEquals(holdingsAsJson.getJsonObject(1).getString("id"), createdItem.getString("holdingId"));
    assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
  }

  @Test
  @SneakyThrows
  void shouldPopulateSameHoldingsItForAllItemsIfOnlyOneHoldingExist() {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String materialTypeId = UUID.randomUUID().toString();
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(materialTypeId),
      StringValue.of("645398607547"));

    String expectedHoldingId1 = UUID.randomUUID().toString();
    JsonArray holdingsAsJson = new JsonArray(List.of(new JsonObject()
      .put("id", expectedHoldingId1)
      .put("permanentLocationId", PERMANENT_LOCATION_ID)));

    String expectedPoLineId = UUID.randomUUID().toString();
    JsonObject poLineAsJson = new JsonObject().put("id", expectedPoLineId);
    HashMap<String, String> payloadContext = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));
    assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      assertNotNull(createdItem.getJsonObject("status").getString("name"));
      assertNotNull(createdItem.getString("permanentLoanTypeId"));
      assertNotNull(createdItem.getString("materialTypeId"));
      assertEquals(expectedHoldingId1, createdItem.getString("holdingId"));
      assertEquals(expectedPoLineId, createdItem.getString("purchaseOrderLineIdentifier"));
    }
  }

  @Test
  void shouldCreateItemAndFillInHoldingsRecordIdFromParsedRecordContent()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));
    payloadContext.put(ERRORS, Json.encode(new PartialError(null, "testError")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(0, errors.size());
    assertEquals(1, createdItems.size());
    JsonObject createdItem = createdItems.getJsonObject(0);
    assertNotNull(createdItem.getJsonObject("status").getString("name"));
    assertNotNull(createdItem.getString("permanentLoanTypeId"));
    assertNotNull(createdItem.getString("materialTypeId"));
    assertEquals(createdItem.getString("holdingId"), expectedHoldingId);
  }

  @Test
  void shouldCreateItemAndFillInHoldingsRecordIdFromMatchedHolding()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String permanentLocationId = UUID.randomUUID().toString();
    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson =
      new JsonObject().put("id", expectedHoldingId).put("permanentLocationId", permanentLocationId);
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(ERRORS, Json.encode(new PartialError(null, "testError")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(0, errors.size());
    assertEquals(1, createdItems.size());
    JsonObject createdItem = createdItems.getJsonObject(0);
    assertNotNull(createdItem.getJsonObject("status").getString("name"));
    assertNotNull(createdItem.getString("permanentLoanTypeId"));
    assertNotNull(createdItem.getString("materialTypeId"));
    assertEquals(createdItem.getString("holdingId"), expectedHoldingId);
  }

  @Test
  void shouldNotReturnFailedFutureIfInventoryStorageErrorExists()
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

    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection)
      .add(argThat(item -> item.getBarcode().equals("645398607547")), any(), any());

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
    assertEquals(errorMsg, errors.getJsonObject(0).getString("error"));
    assertEquals(ITEM_ID, errors.getJsonObject(0).getString("id"));
  }

  @Test
  void shouldCompleteFutureAndReturnErrorsWhenMappedItemWithoutStatus()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    // given
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(""),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
  }

  @Test
  void shouldCompleteAndReturnErrorWhenMappedItemWithUnrecognizedStatusName()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    // given
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of("fakeStatus"),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
  }

  @Test
  void shouldCompleteAndReturnErrorWhenCreatedItemHasExistingBarcode()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection)
      .findByCql(argThat(query -> query.contains("745398607547")), any(PagingParameters.class), any(),
        any());

    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
  }

  @Test
  void shouldCompleteReturnErrorWhenMappedItemWithoutPermanentLoanType()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());
    // given
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("745398607547"),
      StringValue.of(null),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of("645398607547"));
    String permanentLocationId2 = UUID.randomUUID().toString();

    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", UUID.randomUUID().toString())
        .put("permanentLocationId", permanentLocationId2)));

    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    JsonArray errors = new JsonArray(eventPayload.getContext().get(ERRORS));
    assertEquals(1, createdItems.size());
    assertEquals(1, errors.size());
  }

  @Test
  void shouldReturnFailedFutureIfDuplicatedErrorExists() {

    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    // given
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    // given
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldNotRequestWhenCreatedItemHasEmptyBarcode()
    throws UnsupportedEncodingException, ExecutionException, InterruptedException, TimeoutException {

    // given
    doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection)
      .findByCql(argThat(query -> query.contains("745398607547")), any(PagingParameters.class), any(), any());

    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());
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

    MappingProfile marcBibliographicMapping = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Prelim item from MARC")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(Arrays.asList(
          new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
          new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"")
            .withEnabled("true"),
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
            .withProfileId(marcBibliographicMapping.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(marcBibliographicMapping).getMap()))));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);

    assertNotNull(eventPayload);

    // then
    verify(mockedItemCollection, times(0))
      .findByCql(anyString(), any(PagingParameters.class), any(), any());
  }

  @Test
  void shouldReturnFailedFutureWhenHasNoMarcRecord() {

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnFailedFutureWhenCouldNotFindHoldingsRecordIdInEventPayload() {
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITHOUT_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnFailedFutureWhenCouldNotFindPoLineIdInEventPayload() {

    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));
    payloadContext.put(EntityType.PO_LINE.value(), new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldFailWhenNoItemsCreated() {

    // given
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()), StringValue.of(""),
      StringValue.of(UUID.randomUUID().toString()));

    JsonObject holdingAsJson = new JsonObject().put("id", UUID.randomUUID().toString());
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), Json.encode(List.of(holdingAsJson)));
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnFailedFutureWhenCurrentActionProfileHasNoMappingProfile() {
    // given
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withContext(payloadContext)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withContentType(ACTION_PROFILE));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals("Action profile to create an Item requires a mapping profile", exception.getCause().getMessage());
  }

  @Test
  void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    assertTrue(isEligible);
  }

  @Test
  void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile instanceActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper actionProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(instanceActionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(actionProfileSnapshot);

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    assertFalse(isEligible);
  }

  @Test
  void shouldNotProcessEventWhenRecordToItemFutureFails() {
    // given
    when(itemIdStorageService.store(any(), any(), any())).thenReturn(
      Future.failedFuture(new RuntimeException("Something wrong with database!")));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingAsJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnFailedFutureWhenTryingToCreateItemsWithDifferentMaterialTypesDuringCreationOfOpenOrder() {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(EntityType.PO_LINE.value(), poLineAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(ITEMS_SHOULD_HAVE_SAME_MATERIAL_TYPE, exception.getCause().getMessage());
  }

  @Test
  @SneakyThrows
  void shouldCreateMultipleItemsWithDifferentMaterialTypesWhenNoPoLineInTheContext() {
    // given
    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(AVAILABLE.value()),
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
    JsonArray holdingsAsJson = new JsonArray(List.of(new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));
    HashMap<String, String> payloadContext = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertEquals(DI_INVENTORY_ITEM_CREATED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(ITEM.value()));
    assertEquals(EMPTY_JSON_ARRAY, eventPayload.getContext().get(ERRORS));

    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(2, createdItems.size());

    for (int i = 0; i < createdItems.size(); i++) {
      JsonObject createdItem = createdItems.getJsonObject(i);
      assertNotNull(createdItem.getJsonObject("status").getString("name"));
      assertNotNull(createdItem.getString("permanentLoanTypeId"));
      assertNotNull(createdItem.getString("materialTypeId"));
      assertEquals(holdingsAsJson.getJsonObject(i).getString("id"), createdItem.getString("holdingId"));
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldNotCreateItemIfStatisticalCodeIdIsInvalid() {
    MappingProfile invalidStatCodeMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(Arrays.asList(
          new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
          new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"")
            .withEnabled("true"),
          new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"")
            .withEnabled("true"),
          new MappingRule().withName("statisticalCodeId").withPath("item.statisticalCodeIds[]")
            .withValue("invalidStatCodeValue").withEnabled("true")
            .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
        )));

    final ProfileSnapshotWrapper snapshotWrapper = new ProfileSnapshotWrapper()
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
              .withProfileId(invalidStatCodeMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(invalidStatCodeMappingProfile).getMap())))));

    // reader returns: status, permanentLoanType, materialType, then invalid statistical code ID
    when(fakeReader.read(any(MappingRule.class))).thenReturn(
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      ListValue.of(List.of("ebookss")));

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject().put("id", UUID.randomUUID().toString()).put("permanentLocationId", PERMANENT_LOCATION_ID)));

    HashMap<String, String> payloadContext = new HashMap<>();
    Record incomingRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITHOUT_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(snapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    JsonArray errors = new JsonArray(dataImportEventPayload.getContext().get(ERRORS));
    assertEquals(1, errors.size());
    PartialError partialError = errors.getJsonObject(0).mapTo(PartialError.class);
    assertThat(
      partialError.getError(),
      containsString("Provided Statistical code(s) are not a valid values: 'ebookss'.")
    );
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldCreateMultipleItemsAndReturnPartialErrorsForItemWithInvalidStatisticalCode()
    throws ExecutionException, InterruptedException, TimeoutException {
    MappingProfile invalidStatCodeMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ITEM)
      .withMappingDetails(new MappingDetail()
        .withMappingFields(Arrays.asList(
          new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
          new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"")
            .withEnabled("true"),
          new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"")
            .withEnabled("true"),
          new MappingRule().withName("statisticalCodeId").withPath("item.statisticalCodeIds[]")
            .withValue("invalidStatCodeValue").withEnabled("true")
            .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
        )));

    final ProfileSnapshotWrapper snapshotWrapper = new ProfileSnapshotWrapper()
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
              .withProfileId(invalidStatCodeMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(invalidStatCodeMappingProfile).getMap())))));

    String validStatisticalCodeUuid = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      ListValue.of(List.of(validStatisticalCodeUuid)),
      StringValue.of(AVAILABLE.value()),
      StringValue.of(UUID.randomUUID().toString()),
      StringValue.of(UUID.randomUUID().toString()),
      // reader returns the invalid statistical code
      ListValue.of(List.of("ebookss"))
    );

    doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(), any());

    String expectedHoldingId1 = UUID.randomUUID().toString();
    String expectedHoldingId2 = UUID.randomUUID().toString();
    String permanentLocationId2 = UUID.randomUUID().toString();

    JsonArray holdingsAsJson = new JsonArray(List.of(
      new JsonObject()
        .put("id", expectedHoldingId1)
        .put("permanentLocationId", PERMANENT_LOCATION_ID),
      new JsonObject()
        .put("id", expectedHoldingId2)
        .put("permanentLocationId", permanentLocationId2)));

    HashMap<String, String> payloadContext = new HashMap<>();
    Record incomingRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_HOLDING_ID));
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingsAsJson.encode());
    payloadContext.put(MULTIPLE_HOLDINGS_FIELD, "945");
    payloadContext.put(HOLDINGS_IDENTIFIERS, Json.encode(List.of(PERMANENT_LOCATION_ID, permanentLocationId2)));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(snapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    assertNotNull(eventPayload.getContext().get(ITEM.value()));
    JsonArray createdItems = new JsonArray(eventPayload.getContext().get(ITEM.value()));
    assertEquals(1, createdItems.size());
    JsonObject createdItem = createdItems.getJsonObject(0);
    assertNotNull(createdItem.getJsonObject("status").getString("name"));
    assertNotNull(createdItem.getString("permanentLoanTypeId"));
    assertNotNull(createdItem.getString("materialTypeId"));
    assertEquals(holdingsAsJson.getJsonObject(0).getString("id"), createdItem.getString("holdingId"));
    assertTrue(createdItem.getJsonArray("statisticalCodeIds").contains(validStatisticalCodeUuid));

    JsonArray errors = new JsonArray(dataImportEventPayload.getContext().get(ERRORS));
    assertEquals(1, errors.size());
    PartialError partialError = errors.getJsonObject(0).mapTo(PartialError.class);
    assertEquals(expectedHoldingId2, partialError.getHoldingId());
    assertThat(
      partialError.getError(),
      containsString("Provided Statistical code(s) are not a valid values: 'ebookss'.")
    );
  }
}
