package org.folio.inventory.eventhandlers;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.inventory.dataimport.handlers.matching.loaders.AbstractLoader.MULTI_MATCH_IDS;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ReactTo.MATCH;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.dataimport.HoldingsItemMatcherFactory;
import org.folio.processing.matching.MatchingManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.MappingMetadataDto;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.MatchItemEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.ItemLoader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.AbstractPreloader;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

@RunWith(VertxUnitRunner.class)
public class MatchItemEventHandlerUnitTest {

  private static final String ITEM_HRID = "001234";
  private static final String ITEM_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";
  private static final String HOLDING_ID = "9634a5ab-9228-4703-baf2-4d12ebc77d56";

  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MATCHING_RELATIONS = "{\"item.statisticalCodeIds[]\":\"statisticalCode\",\"instance.classifications[].classificationTypeId\":\"classificationTypes\",\"instance.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"item.permanentLoanTypeId\":\"loantypes\",\"holdingsrecord.temporaryLocationId\":\"locations\",\"holdingsrecord.statisticalCodeIds[]\":\"statisticalCode\",\"instance.statusId\":\"instanceStatuses\",\"instance.natureOfContentTermIds\":\"natureOfContentTerms\",\"item.notes[].itemNoteTypeId\":\"itemNoteTypes\",\"holdingsrecord.permanentLocationId\":\"locations\",\"instance.alternativeTitles[].alternativeTitleTypeId\":\"alternativeTitleTypes\",\"holdingsrecord.illPolicyId\":\"illPolicies\",\"item.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.identifiers[].identifierTypeId\":\"identifierTypes\",\"holdingsrecord.holdingsTypeId\":\"holdingsTypes\",\"item.permanentLocationId\":\"locations\",\"instance.modeOfIssuanceId\":\"issuanceModes\",\"item.itemLevelCallNumberTypeId\":\"callNumberTypes\",\"instance.notes[].instanceNoteTypeId\":\"instanceNoteTypes\",\"instance.instanceFormatIds\":\"instanceFormats\",\"holdingsrecord.callNumberTypeId\":\"callNumberTypes\",\"holdingsrecord.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.instanceTypeId\":\"instanceTypes\",\"instance.statisticalCodeIds[]\":\"statisticalCode\",\"instancerelationship.instanceRelationshipTypeId\":\"instanceRelationshipTypes\",\"item.temporaryLoanTypeId\":\"loantypes\",\"item.temporaryLocationId\":\"locations\",\"item.materialTypeId\":\"materialTypes\",\"holdingsrecord.notes[].holdingsNoteTypeId\":\"holdingsNoteTypes\",\"instance.contributors[].contributorNameTypeId\":\"contributorNameTypes\",\"item.itemDamagedStatusId\":\"itemDamageStatuses\",\"instance.contributors[].contributorTypeId\":\"contributorTypes\"}";
  private static final String LOCATIONS_PARAMS = "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"Annex\",\"code\":\"KU/CC/DI/A\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257690,\"updatedDate\":1592219257690}},{\"id\":\"b241764c-1466-4e1d-a028-1a3684a5da87\",\"name\":\"Popular Reading Collection\",\"code\":\"KU/CC/DI/P\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257711,\"updatedDate\":1592219257711}}]}";

  @Mock
  private Storage storage;
  @Mock
  private ItemCollection itemCollection;
  @Mock
  private MarcValueReaderImpl marcValueReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private AbstractPreloader preloader;
  @InjectMocks
  private ItemLoader itemLoader = new ItemLoader(storage, Vertx.vertx(), preloader);

  @Before
  public void setUp() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
    MockitoAnnotations.initMocks(this);
    when(marcValueReader.isEligibleForEntityType(MARC_BIBLIOGRAPHIC)).thenReturn(true);
    when(storage.getItemCollection(any(Context.class))).thenReturn(itemCollection);
    when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
      .thenReturn(StringValue.of(ITEM_HRID));
    MatchValueReaderFactory.register(marcValueReader);
    MatchValueLoaderFactory.register(itemLoader);
    MatchingManager.registerMatcherFactory(new HoldingsItemMatcherFactory());

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(LOCATIONS_PARAMS))));

    doAnswer(invocationOnMock -> CompletableFuture.completedFuture(invocationOnMock.getArgument(0)))
            .when(preloader)
            .preload(any(), any());
  }

  @Test
  public void shouldMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Item>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(singletonList(createItem()), 1));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(eq(format("hrid == \"%s\"", ITEM_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Item>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfMatchedMultipleItems(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Item>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(asList(createItem(), createItem()), 2));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfFailedCallToInventoryStorage(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Failure> callback = ans.getArgument(3);
      Failure result =
        new Failure("Internal Server Error", 500);
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfExceptionThrown(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doThrow(new UnsupportedEncodingException()).when(itemCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchOnHandleEventPayloadIfValueIsMissing(TestContext testContext) {
    Async async = testContext.async();

    when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
      .thenReturn(MissingValue.getInstance());

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfNullCurrentNode() {
    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfCurrentNodeTypeIsNotMatchProfile() {
    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MAPPING_PROFILE));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleForNotItemMatchProfile() {
    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(MARC_BIBLIOGRAPHIC))));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnTrueOnIsEligibleForItemMatchProfile() {
    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(ITEM))));
    assertTrue(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldMatchWithSubMatchByItemOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Item>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(singletonList(createItem()), 1));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == \"%s\"", ITEM_HRID, ITEM_ID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.ITEM.value(), JsonObject.mapFrom(createItem()).encode());
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldMatchWithSubMatchByHoldingOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Item>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(singletonList(createItem()), 1));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(eq(format("hrid == \"%s\" AND holdingsRecordId == \"%s\"", ITEM_HRID, HOLDING_ID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.HOLDINGS.value(), JsonArray.of(new HoldingsRecord().withId(HOLDING_ID)).encode());
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldMatchWithSubConditionBasedOnMultiMatchResultOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();
    List<String> multiMatchResult = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    Item expectedItem = createItem();

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Item>>> callback = invocation.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(singletonList(expectedItem), 1));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == (%s OR %s)", ITEM_HRID, multiMatchResult.get(0), multiMatchResult.get(1))),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    HashMap<String, String> context = new HashMap<>();
    context.put(MULTI_MATCH_IDS, Json.encode(multiMatchResult));
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);

    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, processedPayload.getEventsChain().size());
      testContext.assertEquals(
        processedPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_ITEM_MATCHED.value(), processedPayload.getEventType());
      testContext.assertEquals(
        new JsonObject(processedPayload.getContext().get(ITEM.value())).getString("id"),
        expectedItem.getId());
      async.complete();
    });
  }

  @Test
  public void shouldPutMultipleMatchResultToPayloadOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();
    List<Item> matchedItems = List.of(
      new Item(ITEM_ID, "1", HOLDING_ID, new Status(ItemStatusName.AVAILABLE),
        UUID.randomUUID().toString(), UUID.randomUUID().toString(), new JsonObject()),
      new Item(UUID.randomUUID().toString(), "1", HOLDING_ID, new Status(ItemStatusName.AVAILABLE),
        UUID.randomUUID().toString(), UUID.randomUUID().toString(), new JsonObject()));

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Item>>> callback = invocation.getArgument(2);
      Success<MultipleRecords<Item>> result =
        new Success<>(new MultipleRecords<>(matchedItems, 2));
      callback.accept(result);
      return null;
    }).when(itemCollection)
      .findByCql(eq(format("hrid == \"%s\"", ITEM_HRID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchItemEventHandler(mappingMetadataCache);
    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);
    eventPayload.getCurrentNode().setChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withContent(new MatchProfile().withExistingRecordType(ITEM).withIncomingRecordType(MARC_BIBLIOGRAPHIC))
      .withContentType(MATCH_PROFILE)
      .withReactTo(MATCH)));

    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, processedPayload.getEventsChain().size());
      testContext.assertEquals(DI_INVENTORY_ITEM_MATCHED.value(), processedPayload.getEventType());
      assertThat(new JsonArray(processedPayload.getContext().get(MULTI_MATCH_IDS)),
        hasItems(matchedItems.get(0).getId(), matchedItems.get(1).getId()));
      async.complete();
    }));
  }

  private DataImportEventPayload createEventPayload() {
    return new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventsChain(new ArrayList<>())
      .withOkapiUrl("http://localhost:9493")
      .withTenant("diku")
      .withToken("token")
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(ITEM)
          .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(HOLDINGS)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(singletonList(
                new Field().withLabel("field").withValue("item.hrid"))
              ))))));
  }

  private Item createItem() {
    return new Item(ITEM_ID, "3", HOLDING_ID,
      new Status(ItemStatusName.AVAILABLE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), new JsonObject());
  }

}
