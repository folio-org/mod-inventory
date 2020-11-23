package org.folio.inventory.eventhandlers;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
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
public class MatchInstanceEventHandlerUnitTest {

  private static final String INSTANCE_HRID = "in0001234";
  private static final String INSTANCE_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";

  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MATCHING_RELATIONS = "{\"item.statisticalCodeIds[]\":\"statisticalCode\",\"instance.classifications[].classificationTypeId\":\"classificationTypes\",\"instance.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"item.permanentLoanTypeId\":\"loantypes\",\"holdingsrecord.temporaryLocationId\":\"locations\",\"holdingsrecord.statisticalCodeIds[]\":\"statisticalCode\",\"instance.statusId\":\"instanceStatuses\",\"instance.natureOfContentTermIds\":\"natureOfContentTerms\",\"item.notes[].itemNoteTypeId\":\"itemNoteTypes\",\"holdingsrecord.permanentLocationId\":\"locations\",\"instance.alternativeTitles[].alternativeTitleTypeId\":\"alternativeTitleTypes\",\"holdingsrecord.illPolicyId\":\"illPolicies\",\"item.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.identifiers[].identifierTypeId\":\"identifierTypes\",\"holdingsrecord.holdingsTypeId\":\"holdingsTypes\",\"item.permanentLocationId\":\"locations\",\"instance.modeOfIssuanceId\":\"issuanceModes\",\"item.itemLevelCallNumberTypeId\":\"callNumberTypes\",\"instance.notes[].instanceNoteTypeId\":\"instanceNoteTypes\",\"instance.instanceFormatIds\":\"instanceFormats\",\"holdingsrecord.callNumberTypeId\":\"callNumberTypes\",\"holdingsrecord.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.instanceTypeId\":\"instanceTypes\",\"instance.statisticalCodeIds[]\":\"statisticalCode\",\"instancerelationship.instanceRelationshipTypeId\":\"instanceRelationshipTypes\",\"item.temporaryLoanTypeId\":\"loantypes\",\"item.temporaryLocationId\":\"locations\",\"item.materialTypeId\":\"materialTypes\",\"holdingsrecord.notes[].holdingsNoteTypeId\":\"holdingsNoteTypes\",\"instance.contributors[].contributorNameTypeId\":\"contributorNameTypes\",\"item.itemDamagedStatusId\":\"itemDamageStatuses\",\"instance.contributors[].contributorTypeId\":\"contributorTypes\"}";
  private static final String LOCATIONS_PARAMS = "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"Annex\",\"code\":\"KU/CC/DI/A\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257690,\"updatedDate\":1592219257690}},{\"id\":\"b241764c-1466-4e1d-a028-1a3684a5da87\",\"name\":\"Popular Reading Collection\",\"code\":\"KU/CC/DI/P\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257711,\"updatedDate\":1592219257711}}]}";


  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceCollection;
  @Mock
  private MarcValueReaderImpl marcValueReader;
  @InjectMocks
  private final InstanceLoader instanceLoader = new InstanceLoader(storage, Vertx.vertx());

  @Before
  public void setUp() {
    MatchValueReaderFactory.clearReaderFactory();
    MatchValueLoaderFactory.clearLoaderFactory();
    MockitoAnnotations.initMocks(this);
    when(marcValueReader.isEligibleForEntityType(MARC_BIBLIOGRAPHIC)).thenReturn(true);
    when(storage.getInstanceCollection(any(Context.class))).thenReturn(instanceCollection);
    when(marcValueReader.read(any(DataImportEventPayload.class), any(MatchDetail.class)))
      .thenReturn(StringValue.of(INSTANCE_HRID));
    MatchValueReaderFactory.register(marcValueReader);
    MatchValueLoaderFactory.register(instanceLoader);
  }

  @Test
  public void shouldMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback =
        (Consumer<Success<MultipleRecords<Instance>>>) ans.getArguments()[2];
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback =
        (Consumer<Success<MultipleRecords<Instance>>>) ans.getArguments()[2];
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfMatchedMultipleInstances(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback =
        (Consumer<Success<MultipleRecords<Instance>>>) ans.getArguments()[2];
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(asList(createInstance(), createInstance()), 2));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
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
      Consumer<Failure> callback =
        (Consumer<Failure>) ans.getArguments()[3];
      Failure result =
        new Failure("Internal Server Error", 500);
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfExceptionThrown(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doThrow(new UnsupportedEncodingException()).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
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

    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfNullCurrentNode() {
    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfCurrentNodeTypeIsNotMatchProfile() {
    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MAPPING_PROFILE));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleForNotInstanceMatchProfile() {
    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(MARC_BIBLIOGRAPHIC))));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnTrueOnIsEligibleForInstanceMatchProfile() {
    EventHandler eventHandler = new MatchInstanceEventHandler();
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(INSTANCE))));
    assertTrue(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldMatchWithSubMatchByInstanceOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback =
        (Consumer<Success<MultipleRecords<Instance>>>) ans.getArguments()[2];
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == \"%s\"", INSTANCE_HRID, INSTANCE_ID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    EventHandler eventHandler = new MatchInstanceEventHandler();
    HashMap<String, String> context = new HashMap<>();
    context.put(EntityType.INSTANCE.value(), JsonObject.mapFrom(createInstance()).encode());
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
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  private DataImportEventPayload createEventPayload() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);

    return new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withEventsChain(new ArrayList<>())
      .withOkapiUrl("http://localhost:9493")
      .withTenant("diku")
      .withToken("token")
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(INSTANCE)
          .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(singletonList(
                new Field().withLabel("field").withValue("instance.hrid"))
              ))))));
  }

  private Instance createInstance() {
    return new Instance(INSTANCE_ID, INSTANCE_HRID, "MARC", "Wonderful", "12334");
  }

}
