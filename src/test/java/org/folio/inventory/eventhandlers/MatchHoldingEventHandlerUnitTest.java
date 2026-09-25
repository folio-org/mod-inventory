package org.folio.inventory.eventhandlers;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.inventory.dataimport.handlers.matching.loaders.AbstractLoader.MULTI_MATCH_IDS;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.MATCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.HoldingsItemMatcherFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.MatchHoldingEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.HoldingLoader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.AbstractPreloader;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.matching.MatchingManager;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.HoldingsRecord;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.StaticValueDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class MatchHoldingEventHandlerUnitTest {

  private static final String HOLDINGS_HRID = "ho00001234";
  private static final String HOLDINGS_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String LOCATIONS_PARAMS = """
    {
      "initialized": true,
      "locations": []
    }
    """;

  @Mock
  private Storage storage;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollection;
  @Mock
  private MarcValueReaderImpl marcValueReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private AbstractPreloader preloader;
  @InjectMocks
  private HoldingLoader holdingLoader;

  @BeforeEach
  void setUp() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
    when(marcValueReader.isEligibleForEntityType(MARC_BIBLIOGRAPHIC)).thenReturn(true);
    when(storage.getHoldingsRecordCollection(any(Context.class))).thenReturn(holdingsRecordCollection);
    when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
      .thenReturn(StringValue.of(HOLDINGS_HRID));
    MatchValueReaderFactory.register(marcValueReader);
    MatchValueLoaderFactory.register(holdingLoader);
    MatchingManager.registerMatcherFactory(new HoldingsItemMatcherFactory());

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(LOCATIONS_PARAMS))));

    doAnswer(invocationOnMock -> CompletableFuture.completedFuture(invocationOnMock.getArgument(0)))
      .when(preloader)
      .preload(any(), any());
  }

  @DisplayName("should match a holdings record when the base query resolves a single result")
  @Test
  void shouldMatchOnHandleEventPayload(VertxTestContext testContext) throws UnsupportedEncodingException {
    // arrange
    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> callback = ans.getArgument(2);
      callback.accept(new Success<>(new MultipleRecords<>(singletonList(createHoldingsRecord()), 1)));
      return null;
    }).when(holdingsRecordCollection)
      .findByCql(eq(format("hrid == \"%s\"", HOLDINGS_HRID)), any(PagingParameters.class), any(), any());

    EventHandler eventHandler = new MatchHoldingEventHandler(mappingMetadataCache, null);
    DataImportEventPayload eventPayload = createEventPayload();

    // act
    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> testContext.verify(() -> {
      // assert
      assertNull(throwable);
      assertEquals(1, updatedEventPayload.getEventsChain().size());
      assertEquals(DI_INVENTORY_HOLDING_MATCHED.value(), updatedEventPayload.getEventType());
      testContext.completeNow();
    }));
  }

  @DisplayName("should not match a holdings record when the base query resolves no results")
  @Test
  void shouldNotMatchOnHandleEventPayload(VertxTestContext testContext) throws UnsupportedEncodingException {
    // arrange
    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> callback = ans.getArgument(2);
      callback.accept(new Success<>(new MultipleRecords<>(new ArrayList<>(), 0)));
      return null;
    }).when(holdingsRecordCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(), any());

    EventHandler eventHandler = new MatchHoldingEventHandler(mappingMetadataCache, null);
    DataImportEventPayload eventPayload = createEventPayload();

    // act
    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> testContext.verify(() -> {
      // assert
      assertNull(throwable);
      assertEquals(DI_INVENTORY_HOLDING_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      testContext.completeNow();
    }));
  }

  @DisplayName("should combine a same-type static-value submatch condition into the parent query")
  @Test
  void shouldCombineStaticValueSubMatchConditionIntoParentQueryWhenSameExistingRecordType(
    VertxTestContext testContext) throws UnsupportedEncodingException {
    // arrange
    // First pass at Option 2 (spike): when the next match profile is a "Static value (submatch only)"
    // match against the same existing record type (HOLDINGS) as the parent, its condition is folded
    // into the parent's own CQL instead of waiting for a second MULTI_MATCH_IDS-scoped round trip.
    // Only the combined query is stubbed, so this also proves the parent step issues the combined query.
    HoldingsRecord matchedHoldingsRecord = createHoldingsRecord();
    String permanentLocationId = UUID.randomUUID().toString();

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> callback = invocation.getArgument(2);
      callback.accept(new Success<>(new MultipleRecords<>(singletonList(matchedHoldingsRecord), 1)));
      return null;
    }).when(holdingsRecordCollection)
      .findByCql(eq(format("hrid == \"%s\" AND (permanentLocationId == \"%s\")", HOLDINGS_HRID, permanentLocationId)),
        any(PagingParameters.class), any(), any());

    MatchProfile staticSubMatchProfile = new MatchProfile()
      .withExistingRecordType(HOLDINGS)
      .withIncomingRecordType(EntityType.STATIC_VALUE)
      .withMatchDetails(singletonList(new MatchDetail()
        .withMatchCriterion(EXACTLY_MATCHES)
        .withIncomingMatchExpression(new MatchExpression()
          .withDataValueType(MatchExpression.DataValueType.STATIC_VALUE)
          .withStaticValueDetails(new StaticValueDetails()
            .withStaticValueType(StaticValueDetails.StaticValueType.TEXT)
            .withText(permanentLocationId)))
        .withExistingMatchExpression(new MatchExpression()
          .withDataValueType(VALUE_FROM_RECORD)
          .withFields(singletonList(
            new Field().withLabel("field").withValue("holdings.permanentLocationId"))))));

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, "{}");
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);
    eventPayload.getCurrentNode().setChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withContent(staticSubMatchProfile)
      .withContentType(MATCH_PROFILE)
      .withReactTo(MATCH)));

    EventHandler eventHandler = new MatchHoldingEventHandler(mappingMetadataCache, null);

    // act
    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> testContext.verify(() -> {
      // assert
      assertNull(throwable);
      assertEquals(DI_INVENTORY_HOLDING_MATCHED.value(), processedPayload.getEventType());
      // HoldingsItemMatcher wraps a resolved holdings record as a single-element JSON array
      JsonArray matchedHoldingsAsJson = new JsonArray(processedPayload.getContext().get(HOLDINGS.value()));
      assertEquals(1, matchedHoldingsAsJson.size());
      assertEquals(matchedHoldingsRecord.getId(), matchedHoldingsAsJson.getJsonObject(0).getString("id"));
      assertNull(processedPayload.getContext().get(MULTI_MATCH_IDS));
      testContext.completeNow();
    }));
  }

  private DataImportEventPayload createEventPayload() {
    return new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
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
          .withExistingRecordType(HOLDINGS)
          .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(HOLDINGS)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(singletonList(
                new Field().withLabel("field").withValue("holdings.hrid"))
              ))))));
  }

  private HoldingsRecord createHoldingsRecord() {
    return new HoldingsRecord().withId(HOLDINGS_ID).withHrid(HOLDINGS_HRID);
  }
}
