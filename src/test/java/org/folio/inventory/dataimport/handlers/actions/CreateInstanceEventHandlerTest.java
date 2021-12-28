package org.folio.inventory.dataimport.handlers.actions;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateInstanceEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  public static final String OKAPI_URL = "http://localhost";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "dummy";

  @Mock
  private Storage storage;
  @Mock
  InstanceCollection instanceRecordCollection;
  @Mock
  OkapiHttpClient mockedClient;
  @Mock
  private InstanceIdStorageService instanceIdStorageService;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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

  private CreateInstanceEventHandler createInstanceEventHandler;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.toString())))));


    Vertx vertx = Vertx.vertx();
    createInstanceEventHandler = new CreateInstanceEventHandler(storage,
      new PrecedingSucceedingTitlesHelper(context -> mockedClient), new MappingMetadataCache(vertx,
      vertx.createHttpClient(new HttpClientOptions().setConnectTimeout(3000)), 3600),
      instanceIdStorageService);


    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).add(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> completedStage(createdResponse()))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String instanceId = "4d4545df-b5ba-4031-a031-70b1c1b2fc5d";
    String title = "titleValue";
    RecordToEntity recordToInstance = RecordToEntity.builder().recordId(recordId).entityId(instanceId).build();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.succeededFuture(Optional.of(recordToInstance)));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_CREATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    String actualInstanceId = createdInstance.getString("id");
    assertNotNull(actualInstanceId);
    assertEquals(instanceId, actualInstanceId);
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals("MARC", createdInstance.getString("source"));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(null)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMArcBibliographicIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMarcBibliographicIsEmptyInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfRequiredFieldIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), MissingValue.getInstance());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile));

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("Action profile to create an Instance requires a mapping profile by jobExecutionId: 'null' and recordId: 'null'", exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotInstance() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertTrue(createInstanceEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value(), createInstanceEventHandler.getPostProcessingInitializationEventType());
  }

  @Test(expected = Exception.class)
  public void shouldNotProcessEventWhenRecordToInstanceFutureFails() throws ExecutionException, InterruptedException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = "fe19bae4-da28-472b-be90-d442e2428ead";
    String recordId = "567859ad-505a-400d-a699-0028a1fdbf84";
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    when(instanceIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new Exception()));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl());

    CompletableFuture<DataImportEventPayload> future = createInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  private Response createdResponse() {
    return new Response(HttpStatus.SC_CREATED, null, null, null);
  }
}
