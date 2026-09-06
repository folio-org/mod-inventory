package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import java.io.IOException;
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
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.services.HoldingsIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.HoldingsRecord;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import support.TestUtil;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class CreateMarcHoldingsEventHandlerTest extends BaseWireMockTest {

  private static final String PARSED_CONTENT_WITH_004_FIELD =
    "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"},{\"004\":\"ybp7406411\"}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_CONTENT_WITH_PERMANENT_LOCATION_ID =
    "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"}, {\"852\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"b\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] }},   {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_HOLDINGS_RECORD = "src/test/resources/marc/parsed-holdings-record.json";
  private static final String PERMANENT_LOCATION_ID = "fe19bae4-da28-472b-be90-d442e2428ead";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-holdings-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);
  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(HOLDINGS);
  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression")
          .withEnabled("true"))));
  private final JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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
  @Mock
  private Storage storage;
  @Mock
  private HoldingsRecordCollection holdingsRecordsCollection;
  @Mock
  private HoldingsRecordsSourceCollection holdingsRecordsSourceCollection;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private HoldingsIdStorageService holdingsIdStorageService;
  @Mock
  private HoldingsCollectionService holdingsCollectionService;
  @Mock
  private ConsortiumService consortiumService;
  private CreateMarcHoldingsEventHandler createMarcHoldingsEventHandler;
  private String instanceId;
  private String sourceId;
  private String holdingsId;
  private String recordId;

  @BeforeEach
  void setUp(Vertx vertx) {
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, true);
    createMarcHoldingsEventHandler =
      new CreateMarcHoldingsEventHandler(storage, mappingMetadataCache, holdingsIdStorageService,
        holdingsCollectionService, consortiumService);
    instanceId = String.valueOf(UUID.randomUUID());
    sourceId = String.valueOf(UUID.randomUUID());

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      recordId = String.valueOf(UUID.randomUUID());
      holdingsId = String.valueOf(UUID.randomUUID());
      RecordToEntity recordToHoldings = RecordToEntity.builder().recordId(recordId).entityId(holdingsId).build();
      return Future.succeededFuture(recordToHoldings);
    }).when(holdingsIdStorageService).store(any(), any(), any());

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));
  }

  @Test
  void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(holdingsCollectionService.findInstanceIdByHrid(any(InstanceCollection.class), any())).thenReturn(
      Future.succeededFuture(instanceId));
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(
      Future.succeededFuture(sourceId));

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    marcRecord.setId(recordId);
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS",
      new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject createdHoldings = new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));

    assertEquals(holdingsId, createdHoldings.getString("id"));
    assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    assertNotNull(createdHoldings.getString("id"));
    assertEquals(instanceId, createdHoldings.getString("instanceId"));
    assertEquals(sourceId, createdHoldings.getString("sourceId"));
    assertEquals(PERMANENT_LOCATION_ID, createdHoldings.getString("permanentLocationId"));
  }

  @Test
  void shouldProcessConsortiumLocalHoldingsForSharedInstanceEvent()
    throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(holdingsCollectionService.findSourceIdByName(any(HoldingsRecordsSourceCollection.class), any())).thenReturn(
      Future.succeededFuture(sourceId));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(holdingsCollectionService.findInstanceIdByHrid(any(), any()))
      .thenReturn(Future.failedFuture(new EventProcessingException("")))
      .thenReturn(Future.succeededFuture(instanceId));
    when(consortiumService.getConsortiumConfiguration(any())).thenReturn(Future.succeededFuture(Optional.of(
      new ConsortiumConfiguration("central", ""))));
    when(consortiumService.createShadowInstance(any(), any(), any(ConsortiumConfiguration.class)))
      .thenReturn(Future.succeededFuture(null));

    var holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    var holdingsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    holdingsRecord.setId(recordId);
    var payloadContext = new HashMap<String, String>();
    payloadContext.put("HOLDINGS",
      new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(holdingsRecord));

    var dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    var future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
    var actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);
    var createdHoldings = new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));

    assertEquals(holdingsId, createdHoldings.getString("id"));
    assertEquals(DI_INVENTORY_HOLDING_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    assertNotNull(createdHoldings.getString("id"));
    assertEquals(instanceId, createdHoldings.getString("instanceId"));
    assertEquals(sourceId, createdHoldings.getString("sourceId"));
    assertEquals(PERMANENT_LOCATION_ID, createdHoldings.getString("permanentLocationId"));

    verify(consortiumService).getConsortiumConfiguration(any());
    verify(holdingsCollectionService, times(2)).findInstanceIdByHrid(any(), any());
    verify(consortiumService).createShadowInstance(any(), eq(instanceId), any(ConsortiumConfiguration.class));
  }

  @Test
  void shouldThrowExceptionIfContextIsNull() {
    assertThrows(ExecutionException.class, () -> {
      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
        .withContext(null)
        .withProfileSnapshot(profileSnapshotWrapper)
        .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(5, TimeUnit.MILLISECONDS);
    });
  }

  @Test
  void shouldThrowExceptionIfFolioRecordIsNotMarcHoldings() {
    assertThrows(ExecutionException.class, () -> {
      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
        .withContext(new HashMap<>())
        .withProfileSnapshot(profileSnapshotWrapper)
        .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(5, TimeUnit.MILLISECONDS);
    });
  }

  @Test
  void shouldThrowExceptionIfChildSnapshotWrappersIsEmpty() {
    assertThrows(ExecutionException.class, () -> {
      var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
      Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
      HashMap<String, String> context = new HashMap<>();
      context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

      var snapshotWrapper = new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withProfileId(jobProfile.getId())
        .withContentType(JOB_PROFILE)
        .withContent(jobProfile)
        .withChildSnapshotWrappers(Collections.emptyList());

      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
        .withContext(context)
        .withCurrentNode(snapshotWrapper);

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(10000, TimeUnit.MILLISECONDS);
    });
  }

  @Test
  void shouldThrowExceptionIfPermanentLocationIdIsNull() throws IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()));

    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_004_FIELD));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS",
      new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals("Can`t create Holding entity: 'permanentLocationId' is empty", exception.getCause().getMessage());
  }

  @Test
  void shouldNotProcessEventIfMarcHoldingDoesNotHave004Field() throws IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_PERMANENT_LOCATION_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS",
      new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals("The field 004 for marc holdings must be not null", exception.getCause().getMessage());
  }

  @Test
  void shouldNotProcessEventIfHoldingRecordIsInvalid() {
    assertThrows(ExecutionException.class, () -> {
      MappingProfile marcMappingProfile = new MappingProfile()
        .withId(UUID.randomUUID().toString())
        .withName("Prelim item from MARC")
        .withIncomingRecordType(EntityType.MARC_HOLDINGS)
        .withExistingRecordType(EntityType.HOLDINGS)
        .withMappingDetails(new MappingDetail()
          .withMappingFields(Lists.newArrayList(
            new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression"),
            new MappingRule().withPath("invalidField").withValue("invalidFieldValue"))));

      ProfileSnapshotWrapper holdingProfileSnapshot = new ProfileSnapshotWrapper()
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
                .withProfileId(marcMappingProfile.getId())
                .withContentType(MAPPING_PROFILE)
                .withContent(JsonObject.mapFrom(marcMappingProfile).getMap())))));

      HoldingsRecord holdings = new HoldingsRecord()
        .withId(String.valueOf(UUID.randomUUID()))
        .withHrid(String.valueOf(UUID.randomUUID()))
        .withInstanceId(String.valueOf(UUID.randomUUID()))
        .withSourceId(String.valueOf(UUID.randomUUID()))
        .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
        .withPermanentLocationId(PERMANENT_LOCATION_ID);

      var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
      Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
      HashMap<String, String> context = new HashMap<>();
      context.put("HOLDINGS",
        new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
      context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
        .withContext(context)
        .withCurrentNode(holdingProfileSnapshot.getChildSnapshotWrappers().getFirst());

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(5, TimeUnit.MILLISECONDS);
    });
  }

  @Test
  void shouldNotProcessEventWhenRecordToHoldingsFutureFails() {
    assertThrows(Exception.class, () -> {
      when(holdingsIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new Exception()));

      HoldingsRecord holdings = new HoldingsRecord()
        .withId(String.valueOf(UUID.randomUUID()))
        .withHrid(String.valueOf(UUID.randomUUID()))
        .withInstanceId(String.valueOf(UUID.randomUUID()))
        .withSourceId(String.valueOf(UUID.randomUUID()))
        .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
        .withPermanentLocationId(PERMANENT_LOCATION_ID);

      var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
      Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
      HashMap<String, String> context = new HashMap<>();
      context.put("HOLDINGS",
        new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
      context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
        .withJobExecutionId(UUID.randomUUID().toString())
        .withOkapiUrl(WIRE_MOCK.baseUrl())
        .withContext(context)
        .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(5, TimeUnit.SECONDS);
    });
  }

  @Test
  void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())
        .withContentType(ACTION_PROFILE));

    CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  void shouldNotProcessEventEvenIfDuplicatedInventoryStorageErrorExists() {
    assertThrows(Exception.class, () -> {
      when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
      when(storage.getHoldingsRecordsSourceCollection(any())).thenReturn(holdingsRecordsSourceCollection);
      when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
      doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(UNIQUE_ID_ERROR_MESSAGE, 400));
        return null;
      }).when(holdingsRecordsCollection).add(any(), any(), any());

      HoldingsRecord holdings = new HoldingsRecord()
        .withId(String.valueOf(UUID.randomUUID()))
        .withHrid(String.valueOf(UUID.randomUUID()))
        .withInstanceId(String.valueOf(UUID.randomUUID()))
        .withSourceId(String.valueOf(UUID.randomUUID()))
        .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
        .withPermanentLocationId(PERMANENT_LOCATION_ID);

      var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
      Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
      marcRecord.setId(recordId);
      HashMap<String, String> context = new HashMap<>();
      context.put("HOLDINGS",
        new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
      context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
        .withJobExecutionId(UUID.randomUUID().toString())
        .withOkapiUrl(WIRE_MOCK.baseUrl())
        .withContext(context)
        .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

      CompletableFuture<DataImportEventPayload> future = createMarcHoldingsEventHandler.handle(dataImportEventPayload);
      future.get(5, TimeUnit.SECONDS);
    });
  }

  @Test
  void isEligibleShouldReturnTrue() {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());
    assertTrue(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    ProfileSnapshotWrapper snapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(snapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile createHoldingsProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.DELETE)
      .withFolioRecord(HOLDINGS);
    ProfileSnapshotWrapper actionProfileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(createHoldingsProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(createHoldingsProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(actionProfileSnapshotWrapper);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isEligibleShouldReturnFalseIfRecordIsNotHoldings() {
    ActionProfile createItemProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ITEM);
    ProfileSnapshotWrapper actionProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(createItemProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(createItemProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(actionProfileSnapshot);
    assertFalse(createMarcHoldingsEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isPostProcessingNeededShouldReturnTrue() {
    assertTrue(createMarcHoldingsEventHandler.isPostProcessingNeeded());
  }

  @Test
  void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value(),
      createMarcHoldingsEventHandler.getPostProcessingInitializationEventType());
  }
}
