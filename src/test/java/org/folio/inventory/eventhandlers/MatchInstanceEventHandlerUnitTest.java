package org.folio.inventory.eventhandlers;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.inventory.dataimport.handlers.matching.loaders.AbstractLoader.MULTI_MATCH_IDS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.MATCH;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.handlers.matching.preloaders.PreloadingFields;
import org.folio.processing.exceptions.MatchingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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
import org.folio.inventory.dataimport.handlers.matching.MatchInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.matching.loaders.InstanceLoader;
import org.folio.inventory.dataimport.handlers.matching.preloaders.AbstractPreloader;
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

@RunWith(VertxUnitRunner.class)
public class MatchInstanceEventHandlerUnitTest {

  private static final String INSTANCE_HRID = "in0001234";
  private static final String INSTANCE_ID = "ddd266ef-07ac-4117-be13-d418b8cd6902";

  private static final String ID_FIELD = "id";
  private static final String MAPPING_PARAMS = "MAPPING_PARAMS";
  private static final String RELATIONS = "MATCHING_PARAMETERS_RELATIONS";
  private static final String MATCHING_RELATIONS = "{\"item.statisticalCodeIds[]\":\"statisticalCode\",\"instance.classifications[].classificationTypeId\":\"classificationTypes\",\"instance.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"item.permanentLoanTypeId\":\"loantypes\",\"holdingsrecord.temporaryLocationId\":\"locations\",\"holdingsrecord.statisticalCodeIds[]\":\"statisticalCode\",\"instance.statusId\":\"instanceStatuses\",\"instance.natureOfContentTermIds\":\"natureOfContentTerms\",\"item.notes[].itemNoteTypeId\":\"itemNoteTypes\",\"holdingsrecord.permanentLocationId\":\"locations\",\"instance.alternativeTitles[].alternativeTitleTypeId\":\"alternativeTitleTypes\",\"holdingsrecord.illPolicyId\":\"illPolicies\",\"item.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.identifiers[].identifierTypeId\":\"identifierTypes\",\"holdingsrecord.holdingsTypeId\":\"holdingsTypes\",\"item.permanentLocationId\":\"locations\",\"instance.modeOfIssuanceId\":\"issuanceModes\",\"item.itemLevelCallNumberTypeId\":\"callNumberTypes\",\"instance.notes[].instanceNoteTypeId\":\"instanceNoteTypes\",\"instance.instanceFormatIds\":\"instanceFormats\",\"holdingsrecord.callNumberTypeId\":\"callNumberTypes\",\"holdingsrecord.electronicAccess[].relationshipId\":\"electronicAccessRelationships\",\"instance.instanceTypeId\":\"instanceTypes\",\"instance.statisticalCodeIds[]\":\"statisticalCode\",\"instancerelationship.instanceRelationshipTypeId\":\"instanceRelationshipTypes\",\"item.temporaryLoanTypeId\":\"loantypes\",\"item.temporaryLocationId\":\"locations\",\"item.materialTypeId\":\"materialTypes\",\"holdingsrecord.notes[].holdingsNoteTypeId\":\"holdingsNoteTypes\",\"instance.contributors[].contributorNameTypeId\":\"contributorNameTypes\",\"item.itemDamagedStatusId\":\"itemDamageStatuses\",\"instance.contributors[].contributorTypeId\":\"contributorTypes\"}";
  private static final String LOCATIONS_PARAMS = "{\"initialized\":true,\"locations\":[{\"id\":\"53cf956f-c1df-410b-8bea-27f712cca7c0\",\"name\":\"Annex\",\"code\":\"KU/CC/DI/A\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257690,\"updatedDate\":1592219257690}},{\"id\":\"b241764c-1466-4e1d-a028-1a3684a5da87\",\"name\":\"Popular Reading Collection\",\"code\":\"KU/CC/DI/P\",\"isActive\":true,\"institutionId\":\"40ee00ca-a518-4b49-be01-0638d0a4ac57\",\"campusId\":\"62cf76b7-cca5-4d33-9217-edf42ce1a848\",\"libraryId\":\"5d78803e-ca04-4b4a-aeae-2c63b924518b\",\"primaryServicePoint\":\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\",\"servicePointIds\":[\"3a40852d-49fd-4df2-a1f9-6e2641a6e91f\"],\"servicePoints\":[],\"metadata\":{\"createdDate\":1592219257711,\"updatedDate\":1592219257711}}]}";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";


  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceCollection;
  @Mock
  private MarcValueReaderImpl marcValueReader;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private ConsortiumService consortiumService;
  @Mock
  private AbstractPreloader preloader;
  private EventHandler eventHandler;
  @InjectMocks
  private final InstanceLoader instanceLoader = new InstanceLoader(storage, preloader);

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

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(LOCATIONS_PARAMS))));

    doAnswer(invocationOnMock -> CompletableFuture.completedFuture(invocationOnMock.getArgument(0)))
            .when(preloader)
            .preload(any(), any());

    eventHandler = new MatchInstanceEventHandler(mappingMetadataCache, consortiumService);

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.empty())).when(consortiumService).getConsortiumConfiguration(any());
  }

  @Test
  public void shouldMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldThrowExceptionWhenMatchOnLocalAndCentralTenant(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    Instance instance = new Instance(UUID.randomUUID().toString(), 5, INSTANCE_HRID, "MARC", "Wonderful", "12334");

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(instance), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      testContext.assertTrue(throwable.getCause() instanceof MatchingException);
      testContext.assertTrue(throwable.getMessage().contains("Found multiple entities during matching"));
      async.complete();
    });
  }

  @Test
  public void shouldNotThrowMultiMatchExceptionIfMatchedShadowInstanceLocallyAndSharedInstanceOnCentralTenant(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    Instance instance = new Instance(INSTANCE_ID, 5, UUID.randomUUID().toString(), "CONSORTIUM-MARC", "Wonderful", "12334");

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(instance), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      JsonObject matchedInstanceAsJsonObject = new JsonObject(updatedEventPayload.getContext().get(INSTANCE.value()));
      testContext.assertEquals(matchedInstanceAsJsonObject.getString(ID_FIELD), INSTANCE_ID);
      testContext.assertEquals(matchedInstanceAsJsonObject.getString("hrid"), INSTANCE_HRID);
      async.complete();
    });
  }

  @Test
  public void shouldMatchOnLocalAndNotMatchCentralTenant(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      JsonObject matchedInstanceAsJsonObject = new JsonObject(updatedEventPayload.getContext().get(INSTANCE.value()));
      testContext.assertEquals(matchedInstanceAsJsonObject.getString(ID_FIELD), INSTANCE_ID);
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchOnLocalAndMatchOnCentralTenant(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    Instance instance = new Instance(UUID.randomUUID().toString(), 5, INSTANCE_HRID, "MARC", "Wonderful", "12334");

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(instance), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      JsonObject matchedInstanceAsJsonObject = new JsonObject(updatedEventPayload.getContext().get(INSTANCE.value()));
      testContext.assertEquals(matchedInstanceAsJsonObject.getString(ID_FIELD), instance.getId());
      testContext.assertEquals(centralTenantId, updatedEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));
      async.complete();
    });
  }

  @Test
  public void shouldNotTryToMatchOnCentralTenantIfMatchingByPol(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    Instance instance = new Instance(UUID.randomUUID().toString(), 5, INSTANCE_HRID, "MARC", "Wonderful", "12334");

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(instance), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("%s == \"%s\"", PreloadingFields.POL.getExistingMatchField(), INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("%s == \"%s\"", PreloadingFields.POL.getExistingMatchField(), INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload("instance." + PreloadingFields.POL.getExistingMatchField());

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      JsonObject matchedInstanceAsJsonObject = new JsonObject(updatedEventPayload.getContext().get(INSTANCE.value()));
      testContext.assertEquals(matchedInstanceAsJsonObject.getString(ID_FIELD), INSTANCE_ID);
      verify(storage, times(0)).getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)));
      async.complete();
    });
  }

  @Test
  public void shouldNotTryToMatchOnCentralTenantIfMatchingByVrn(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String centralTenantId = "consortium";
    String consortiumId = "consortiumId";

    Instance instance = new Instance(UUID.randomUUID().toString(), 5, INSTANCE_HRID, "MARC", "Wonderful", "12334");

    InstanceCollection instanceCollectionCentralTenant = Mockito.mock(InstanceCollection.class);
    when(storage.getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)))).thenReturn(instanceCollectionCentralTenant);

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(instance), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollectionCentralTenant)
      .findByCql(eq(format("%s == \"%s\"", PreloadingFields.VRN.getExistingMatchField(), INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("%s == \"%s\"", PreloadingFields.VRN.getExistingMatchField(), INSTANCE_HRID)), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId))))
      .when(consortiumService).getConsortiumConfiguration(any());

    DataImportEventPayload eventPayload = createEventPayload("instance." + PreloadingFields.VRN.getExistingMatchField());

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      JsonObject matchedInstanceAsJsonObject = new JsonObject(updatedEventPayload.getContext().get(INSTANCE.value()));
      testContext.assertEquals(matchedInstanceAsJsonObject.getString(ID_FIELD), INSTANCE_ID);
      verify(storage, times(0)).getInstanceCollection(Mockito.argThat(context -> context.getTenantId().equals(centralTenantId)));
      async.complete();
    });
  }

  @Test
  public void shouldNotMatchOnHandleEventPayload(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(new ArrayList<>(), 0));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldFailOnHandleEventPayloadIfMatchedMultipleInstances(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(asList(createInstance(), createInstance()), 2));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldIncludeUuidsInErrorMessageWhenFourOrFewerMatchesFound(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    Instance instance1 = new Instance(uuid1, 1, "test123", "MARC", "Test Title", "12345");
    Instance instance2 = new Instance(uuid2, 1, "test123", "MARC", "Test Title", "12345");

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(asList(instance1, instance2), 2));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      String errorMessage = throwable.getCause().getMessage();
      testContext.assertTrue(errorMessage.contains("UUIDs:"));
      testContext.assertTrue(errorMessage.contains(uuid1));
      testContext.assertTrue(errorMessage.contains(uuid2));
      async.complete();
    });
  }

  @Test
  public void shouldIncludeCountInErrorMessageWhenMoreThanFourMatchesFound(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    List<Instance> instances = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      instances.add(new Instance(UUID.randomUUID().toString(), 1, "test" + i, "MARC", "Test Title", "12345"));
    }

    doAnswer(ans -> {
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(instances, 5));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      String errorMessage = throwable.getCause().getMessage();
      testContext.assertTrue(errorMessage.contains("(5 records)"));
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
    }).when(instanceCollection)
      .findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

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

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, updatedEventPayload.getEventsChain().size());
      testContext.assertEquals(
        updatedEventPayload.getEventsChain(),
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldSetInstanceNotMatchedEventToEventPayloadOnHandleIfFailedToGetMappingMetadata(TestContext testContext) {
    Async async = testContext.async();
    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.failedFuture("test error"));

    DataImportEventPayload eventPayload = createEventPayload();

    eventHandler.handle(eventPayload).whenComplete((updatedEventPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      testContext.assertEquals(DI_INVENTORY_INSTANCE_NOT_MATCHED.value(), eventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfNullCurrentNode() {
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleIfCurrentNodeTypeIsNotMatchProfile() {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MAPPING_PROFILE));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnFalseOnIsEligibleForNotInstanceMatchProfile() {
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(JsonObject.mapFrom(new MatchProfile()
          .withExistingRecordType(MARC_BIBLIOGRAPHIC))));
    assertFalse(eventHandler.isEligible(eventPayload));
  }

  @Test
  public void shouldReturnTrueOnIsEligibleForInstanceMatchProfile() {
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
      Consumer<Success<MultipleRecords<Instance>>> callback = ans.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(createInstance()), 1));
      callback.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == \"%s\"", INSTANCE_HRID, INSTANCE_ID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

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
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), updatedEventPayload.getEventType());
      async.complete();
    });
  }

  @Test
  public void shouldMatchWithSubConditionBasedOnMultiMatchResultOnHandleEventPayload(TestContext testContext)
    throws UnsupportedEncodingException {
    Async async = testContext.async();

    List<String> multiMatchResult = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    Instance expectedInstance = createInstance();

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocation.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(expectedInstance), 1));
      successHandler.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == (%s OR %s)", INSTANCE_HRID, multiMatchResult.get(0), multiMatchResult.get(1))),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

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
        singletonList(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      );
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), processedPayload.getEventType());
      testContext.assertEquals(new JsonObject(processedPayload.getContext().get(INSTANCE.value())).getString(ID_FIELD), expectedInstance.getId());
      async.complete();
    });
  }

  @Test
  public void shouldPutMultipleMatchResultToPayloadOnHandleEventPayload(TestContext testContext)
    throws UnsupportedEncodingException {
    Async async = testContext.async();
    List<Instance> matchedInstances = List.of(
      new Instance(UUID.randomUUID().toString(), 1, "in1", "MARC", "Wonderful", "12334"),
      new Instance(UUID.randomUUID().toString(), 1, "in2", "MARC", "Wonderful", "12334"));

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocation.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(matchedInstances, 2));
      successHandler.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MatchProfile subMatchProfile = new MatchProfile()
      .withExistingRecordType(INSTANCE)
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC);

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);
    eventPayload.getCurrentNode().setChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withContent(subMatchProfile)
      .withContentType(MATCH_PROFILE)
      .withReactTo(MATCH)));

    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> testContext.verify(v -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, processedPayload.getEventsChain().size());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), processedPayload.getEventType());
      assertThat(new JsonArray(processedPayload.getContext().get(MULTI_MATCH_IDS)),
        hasItems(matchedInstances.get(0).getId(), matchedInstances.get(1).getId()));
      async.complete();
    }));
  }

  @Test
  public void shouldReturnFailedFutureWhenFirstChildProfileIsNotMatchProfileOnHandleEventPayload(TestContext testContext)
    throws UnsupportedEncodingException {
    Async async = testContext.async();
    List<Instance> matchedInstances = List.of(
      new Instance(UUID.randomUUID().toString(), 1, "in1", "MARC", "Wonderful", "12334"),
      new Instance(UUID.randomUUID().toString(), 1, "in2", "MARC", "Wonderful", "12334"));

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocation.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(matchedInstances, 2));
      successHandler.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\"", INSTANCE_HRID)),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> context = new HashMap<>();
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);
    eventPayload.getCurrentNode().setChildSnapshotWrappers(List.of(
      new ProfileSnapshotWrapper().withContentType(ACTION_PROFILE).withReactTo(MATCH).withOrder(0),
      new ProfileSnapshotWrapper().withContentType(MATCH_PROFILE).withReactTo(MATCH).withOrder(1)));

    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> {
      testContext.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldMatchWithSubConditionBasedOnMarcBibMultipleMatchResult(TestContext testContext)
    throws UnsupportedEncodingException {
    Async async = testContext.async();

    List<String> marcBibMultiMatchResult = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    Instance expectedInstance = createInstance();

    doAnswer(invocation -> {
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocation.getArgument(2);
      Success<MultipleRecords<Instance>> result =
        new Success<>(new MultipleRecords<>(singletonList(expectedInstance), 1));
      successHandler.accept(result);
      return null;
    }).when(instanceCollection)
      .findByCql(eq(format("hrid == \"%s\" AND id == (%s OR %s)", INSTANCE_HRID, marcBibMultiMatchResult.get(0), marcBibMultiMatchResult.get(1))),
        any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCES_IDS_KEY, Json.encode(marcBibMultiMatchResult));
    context.put(MAPPING_PARAMS, LOCATIONS_PARAMS);
    context.put(RELATIONS, MATCHING_RELATIONS);
    DataImportEventPayload eventPayload = createEventPayload().withContext(context);

    eventHandler.handle(eventPayload).whenComplete((processedPayload, throwable) -> {
      testContext.assertNull(throwable);
      testContext.assertEquals(1, processedPayload.getEventsChain().size());
      testContext.assertEquals(DI_INVENTORY_INSTANCE_MATCHED.value(), processedPayload.getEventType());
      testContext.assertEquals(expectedInstance.getId(), new JsonObject(processedPayload.getContext().get(INSTANCE.value())).getString(ID_FIELD));
      testContext.assertNull(processedPayload.getContext().get(INSTANCES_IDS_KEY));
      async.complete();
    });
  }

  private DataImportEventPayload createEventPayload() {
    return createEventPayload("instance.hrid");
  }

  private DataImportEventPayload createEventPayload(String matchValue) {
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
          .withExistingRecordType(INSTANCE)
          .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(singletonList(
                new Field().withLabel("field").withValue(matchValue))
              ))))));
  }

  private Instance createInstance() {
    return new Instance(INSTANCE_ID, 5, INSTANCE_HRID, "MARC", "Wonderful", "12334");
  }

}
