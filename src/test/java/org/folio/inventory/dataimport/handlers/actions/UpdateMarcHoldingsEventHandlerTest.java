package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.instances.Instance;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class UpdateMarcHoldingsEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-holdings-rules.json";
  private static final String PARSED_HOLDINGS_RECORD = "src/test/resources/marc/parsed-holdings-record.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  private static final String PARSED_CONTENT_WITH_004_FIELD = "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"}, {\"852\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"b\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] }},   {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PERMANENT_LOCATION_ID = "fe19bae4-da28-472b-be90-d442e2428ead";

  private final Vertx vertx = Vertx.vertx();
  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));
  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update item-SR")
    .withAction(MODIFY)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);
  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC bib")
    .withIncomingRecordType(EntityType.MARC_HOLDINGS)
    .withExistingRecordType(EntityType.MARC_HOLDINGS)
    .withMappingDetails(new MappingDetail());
  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withProfileId(actionProfile.getId())
    .withContentType(ACTION_PROFILE)
    .withContent(JsonObject.mapFrom(actionProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));
  @Mock
  private HoldingsRecordCollection holdingsCollection;

  @Mock
  private InstanceCollection instanceRecordCollection;

  @Mock
  private Storage storage;
  @Mock
  private KafkaEventPublisher publisher;

  private UpdateMarcHoldingsEventHandler eventHandler;
  private JsonObject mappingRules;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(vertx, vertx.createHttpClient(), true);
    eventHandler = new UpdateMarcHoldingsEventHandler(storage, mappingMetadataCache, publisher);
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Void>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(holdingsCollection).update(any(), any(), any());

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(holdingsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(new HoldingsRecord().withVersion(1)));
    var instanceId = String.valueOf(UUID.randomUUID());
    mockSuccessFindByCql(instanceId, instanceRecordCollection);
    var holdingsId = UUID.randomUUID();
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);

    verify(publisher, times(1)).publish(actualDataImportEventPayload);
    assertNull(actualDataImportEventPayload.getCurrentNode());
    assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonObject holdings = new JsonObject(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    assertNotNull(holdings.getString("id"));
    assertEquals(holdingsId.toString(), holdings.getString("id"));
    assertEquals("1", holdings.getString("_version"));

    assertNotNull(holdings.getString("id"));
    assertNotNull(holdings.getString("instanceId"));
    assertNotNull(holdings.getString("sourceId"));
    assertEquals(PERMANENT_LOCATION_ID, holdings.getString("permanentLocationId"));
  }

  @Test
  public void shouldNotProcessEventIfMarcHoldingDoesNotHave004Field() throws IOException {

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_004_FIELD));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    Assert.assertEquals("The field 004 for marc holdings must be not null", exception.getCause().getMessage());
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsNull() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcHoldingsIsEmptyInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcHoldingsIsNotInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put("Test_Value", "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(), containsString("Unexpected payload"));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void isEligibleShouldReturnTrue() throws IOException {
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(new HashMap<>());
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(HOLDINGS);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotHoldings() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ITEM);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertFalse(eventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals("DI_INVENTORY_HOLDINGS_UPDATED_READY_FOR_POST_PROCESSING", eventHandler.getPostProcessingInitializationEventType());
  }

  @Test
  public void shouldNotProcessEventIfOptimisticLockingErrorExist() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    var instanceId = String.valueOf(UUID.randomUUID());
    mockSuccessFindByCql(instanceId, instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update Holdings record because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(holdingsCollection).update(any(), any(), any());
    when(holdingsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(new HoldingsRecord().withId(UUID.randomUUID().toString()).withVersion(1)));

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));
    context.put("CURRENT_RETRY_NUMBER", "2");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);
    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(),
      containsString("Current retry number 1 exceeded or equal given number 2 for the Holding update"));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldNotProcessEventIfHoldingsNotUpdatedToInventoryStorage() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    var instanceId = String.valueOf(UUID.randomUUID());
    mockSuccessFindByCql(instanceId, instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Unprocessable Entity", 422));
      return null;
    }).when(holdingsCollection).update(any(), any(), any());
    when(holdingsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(new HoldingsRecord().withId(UUID.randomUUID().toString()).withVersion(1)));

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);
    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(), containsString("Error updating Holding by holdingId " + holdingsId));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldProcessEventSecondRetryIfOLErrorExist() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    var instanceId = String.valueOf(UUID.randomUUID());
    mockSuccessFindByCql(instanceId, instanceRecordCollection);
    doAnswer(new Answer() {
      private int count = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (count++ == 0) {
          Consumer<Failure> failureHandler = invocation.getArgument(2);
          failureHandler.accept(new Failure(
            "Cannot update Holdings record because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1",
            409));
        } else {
          Consumer<Success<Void>> successConsumer = invocation.getArgument(1);
          successConsumer.accept(new Success<>(null));
        }
        return null;
      }
    }).when(holdingsCollection).update(any(), any(), any());

    when(holdingsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(new HoldingsRecord().withId(UUID.randomUUID().toString()).withVersion(1)));

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);
    verify(publisher, times(2)).publish(actualDataImportEventPayload);
  }

  @Test
  public void shouldNotProcessEventIfMarcHoldingDoesNotHaveParsedRecord() throws IOException {

    HoldingsRecord holdings = new HoldingsRecord()
      .withId(String.valueOf(UUID.randomUUID()))
      .withHrid(String.valueOf(UUID.randomUUID()))
      .withInstanceId(String.valueOf(UUID.randomUUID()))
      .withSourceId(String.valueOf(UUID.randomUUID()))
      .withHoldingsTypeId(String.valueOf(UUID.randomUUID()))
      .withPermanentLocationId(PERMANENT_LOCATION_ID);

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(null));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(holdings)).encode());
    context.put(MARC_HOLDINGS.value(), Json.encode(record));
    context.put("MAPPING_RULES", mappingRules.encode());
    context.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    Assert.assertEquals("Error in default mapper.", exception.getCause().getMessage());
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldNotProcessEventIfFindByCQLHasInternalServerError() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(), containsString("Internal Server Error"));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldNotProcessEventIfFindByCQLTotalRecordsIsZero() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doAnswer(invocationOnMock -> {
      List<Instance> instanceList = Collections.emptyList();
      MultipleRecords<Instance> result = new MultipleRecords<>(instanceList, 0);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(), containsString("No instance id found for marc holdings with hrid: in00000000315"));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldNotProcessEventIfFindByCQLThrowsUnsupportedEncodingException() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    doThrow(new UnsupportedEncodingException("Unsupported encoding.")).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    var holdingsId = UUID.randomUUID();
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    var future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getCause().getMessage(), containsString("Unsupported encoding."));
    verify(publisher, times(0)).publish(any());
  }

  @Test
  public void shouldReturnFailedFutureIfHasNoHoldingByIdFromSourceRecord() throws IOException {
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsCollection);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(holdingsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    var instanceId = String.valueOf(UUID.randomUUID());
    mockSuccessFindByCql(instanceId, instanceRecordCollection);
    var holdingsId = UUID.randomUUID();
    var parsedHoldingsRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_HOLDINGS_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedHoldingsRecord.encode()));
    record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingsId.toString()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage(), containsString("Holdings record was not found"));
    verify(publisher, times(0)).publish(any());
  }


  @SneakyThrows
  private void mockSuccessFindByCql(String instanceId, InstanceCollection instanceRecordCollection) {
    doAnswer(invocationOnMock -> {
      Instance instance = new Instance(instanceId, 2, String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      List<Instance> instanceList = Collections.singletonList(instance);
      MultipleRecords<Instance> result = new MultipleRecords<>(instanceList, 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
  }
}
