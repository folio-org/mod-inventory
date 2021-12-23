package org.folio.inventory.eventhandlers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.matching.MatchAuthorityEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.AuthorityLoader;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.matching.loader.MatchValueLoaderFactory;
import org.folio.processing.matching.reader.MarcValueReaderImpl;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.rest.jaxrs.model.EntityType.AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class MatchAuthorityEventHandlerUnitTest {

  private static final String AUTHORITY_ID = "3217f3f2-6a7b-467a-b421-4e9fe0643cd7";
  private static final String PERSONAL_NAME = "Twain, Mark, 1835-1910";
  @Mock
  private Storage storage;
  @InjectMocks
  private final AuthorityLoader loader = new AuthorityLoader(storage, Vertx.vertx());
  @Mock
  private AuthorityRecordCollection collection;
  @Mock
  private MarcValueReaderImpl marcValueReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;

  @Before
  public void setUp() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
    MockitoAnnotations.initMocks(this);
    when(marcValueReader.isEligibleForEntityType(MARC_AUTHORITY)).thenReturn(true);
    when(storage.getAuthorityRecordCollection(any(Context.class))).thenReturn(collection);
    when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
      .thenReturn(StringValue.of(PERSONAL_NAME));
    MatchValueReaderFactory.register(marcValueReader);
    MatchValueLoaderFactory.register(loader);

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(new JsonObject().encode()))));
  }

  @Test
  public void shouldMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Authority>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Authority>> result =
        new Success<>(new MultipleRecords<>(singletonList(createAuthority()), 1));
      callback.accept(result);
      return null;
    }).when(collection)
      .findByCql(eq(format("personalName == \"%s\"", PERSONAL_NAME)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_AUTHORITY_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });

  }

  @Test
  public void shouldNotMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Authority>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Authority>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(collection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_AUTHORITY_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfMatchedMultipleAuthorityRecords(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Authority>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Authority>> result =
        new Success<>(new MultipleRecords<>(asList(createAuthority(), createAuthority()), 2));
      callback.accept(result);
      return null;
    }).when(collection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
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
    }).when(collection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfExceptionThrown(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doThrow(new UnsupportedEncodingException()).when(collection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
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

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_AUTHORITY_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfNullCurrentNode() {
    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfCurrentNodeTypeIsNotMatchProfile() {
    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MAPPING_PROFILE));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleForNotAuthorityMatchProfile() {
    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(MARC_AUTHORITY))));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnTrueOnIsEligibleForAuthorityMatchProfile() {
    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(AUTHORITY))));
    assertTrue(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldMatchWithSubMatchByAuthorityOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Authority>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Authority>> result =
        new Success<>(new MultipleRecords<>(singletonList(createAuthority()), 1));
      callback.accept(result);
      return null;
    }).when(collection)
      .findByCql(eq(format("personalName == \"%s\" AND id == \"%s\"", PERSONAL_NAME, AUTHORITY_ID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchAuthorityEventHandler(mappingMetadataCache);
    HashMap<String, String> context = new HashMap<>();
    context.put(AUTHORITY.value(), JsonObject.mapFrom(createAuthority()).encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_AUTHORITY_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });

  }

  private Authority createAuthority() {
    return new Authority().withId(AUTHORITY_ID).withPersonalName(PERSONAL_NAME);
  }

  private DataImportEventPayload createEventPayload() {
    return new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventsChain(new ArrayList<>())
      .withOkapiUrl("http://localhost:00001")
      .withTenant("diku")
      .withToken("token")
      .withContext(new HashMap<>())
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(AUTHORITY)
          .withIncomingRecordType(MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(singletonList(
                new Field().withLabel("field").withValue("authority.personalName"))
              ))))));
  }
}
