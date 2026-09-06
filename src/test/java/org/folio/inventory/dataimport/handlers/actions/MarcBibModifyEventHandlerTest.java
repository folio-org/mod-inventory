package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.DeleteRuleFor999FieldCache;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.modify.MarcBibModifyEventHandler;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import support.TestUtil;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MarcBibModifyEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";

  private final MarcMappingDetail marcMappingDetail = new MarcMappingDetail()
    .withOrder(0)
    .withAction(MarcMappingDetail.Action.ADD)
    .withField(new MarcField()
      .withField("856")
      .withIndicator1(null)
      .withIndicator2(null)
      .withSubfields(Collections.singletonList(new MarcSubfield()
        .withSubfield("u")
        .withSubaction(MarcSubfield.Subaction.INSERT)
        .withPosition(MarcSubfield.Position.BEFORE_STRING)
        .withData(new Data().withText("http://libproxy.smith.edu?url=")))));

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update item-SR")
    .withAction(MODIFY)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC bib")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(Collections.singletonList(marcMappingDetail))
      .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY));

  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withProfileId(actionProfile.getId())
    .withContentType(ACTION_PROFILE)
    .withContent(JsonObject.mapFrom(actionProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private OkapiHttpClient mockedOkapiHttpClient;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private HttpResponse<Buffer> putRecordHttpResponse;

  private Record marcRecord;
  private Instance existingInstance;

  private MarcBibModifyEventHandler marcBibModifyEventHandler;

  @BeforeEach
  void setUp() {
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    marcRecord = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    marcRecord.getParsedRecord().withContent(JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).encode());

    Vertx vertx = Vertx.vertx();
    HttpClient httpClient = vertx.createHttpClient();

    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);

    when(mockedOkapiHttpClient.delete(anyString()))
      .thenReturn(CompletableFuture.completedFuture(new Response(204, null, null, null)));

    when(mockedOkapiHttpClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(getOkResponse(new JsonObject().encode())));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Instance instance = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedInstanceCollection).update(any(Instance.class), any(Consumer.class), any(Consumer.class));

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH)).encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    when(putRecordHttpResponse.statusCode()).thenReturn(HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(putRecordHttpResponse));

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper =
      new PrecedingSucceedingTitlesHelper(ctxt -> mockedOkapiHttpClient);
    DeleteRuleFor999FieldCache deleteRuleFor999FieldCache = DeleteRuleFor999FieldCache.getInstance(vertx, true);
    marcBibModifyEventHandler = spy(new MarcBibModifyEventHandler(mappingMetadataCache, deleteRuleFor999FieldCache,
      new InstanceUpdateDelegate(mockedStorage), precedingSucceedingTitlesHelper, httpClient));

    doReturn(sourceStorageClient).when(marcBibModifyEventHandler).getSourceStorageRecordsClient(any());
  }

  @Test
  void shouldModifyRecordAndUpdateInstance() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField =
      "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);
    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertTrue(addedField.isPresent());
    assertEquals(expectedAddedField, addedField.get().encode());
    assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value())).findFirst().get());
    assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .get());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().getFirst());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().getFirst());
    assertNotNull(updatedInstance.getSubjects());
    assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().getFirst().getValue(), Matchers.containsString("additional subfield"));
    assertNotNull(updatedInstance.getNotes());
    assertEquals("Adding a note", updatedInstance.getNotes().getFirst().note());

    verify(mockedInstanceCollection).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(marcRecord.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString()
        .equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  void shouldModifyRecordAnNotUpdateInstanceIfEntityDoesNotExistAtContext()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    String expectedAddedField =
      "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertTrue(addedField.isPresent());
    assertEquals(expectedAddedField, addedField.get().encode());
    assertFalse(eventPayload.getContext().containsKey(INSTANCE.value()));

    verify(mockedInstanceCollection, never()).update(any(), any(), any());
    verify(sourceStorageClient, never()).putSourceStorageRecordsById(any(), any());
  }

  @Test
  void shouldModifyMarcBibAndUpdateInstanceAtCentralTenantIfCentralTenantIdExistsInContext()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put("CENTRAL_TENANT_ID", CENTRAL_TENANT_ID);
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField =
      "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);

    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    assertTrue(addedField.isPresent());
    assertEquals(expectedAddedField, addedField.get().encode());
    verify(mappingMetadataCache).get(eq(dataImportEventPayload.getJobExecutionId()),
      argThat(context -> context.getTenantId().equals(TENANT_ID)));
    verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value())).findFirst().get());
    assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .get());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().getFirst());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().getFirst());
    assertNotNull(updatedInstance.getSubjects());
    assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().getFirst().getValue(), Matchers.containsString("additional subfield"));
    assertNotNull(updatedInstance.getNotes());
    assertEquals("Adding a note", updatedInstance.getNotes().getFirst().note());

    verify(marcBibModifyEventHandler).getSourceStorageRecordsClient(
      argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
    verify(mockedInstanceCollection).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(marcRecord.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString()
        .equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  void shouldModifyRecordAndUpdateInstanceAfterOptimisticLockingProcessing()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField =
      "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(
        "Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1",
        409));
      return null;
    }).doAnswer(invocationOnMock -> {
      Instance instance = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedInstanceCollection).update(any(), any(), any());

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);
    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    assertTrue(addedField.isPresent());
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(expectedAddedField, addedField.get().encode());
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    assertNotNull(
      updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value())).findFirst().get());
    assertNotNull(
      updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst()
        .get());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().getFirst());
    assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().getFirst());
    assertNotNull(updatedInstance.getSubjects());
    assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().getFirst().getValue(), Matchers.containsString("additional subfield"));
    assertNotNull(updatedInstance.getNotes());
    assertEquals("Adding a note", updatedInstance.getNotes().getFirst().note());

    verify(mockedInstanceCollection, times(2)).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(marcRecord.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString()
        .equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  void shouldRemovePrecedingTitlesOnInstanceUpdateWhenIncomingRecordHasNot()
    throws InterruptedException, ExecutionException {
    // given
    JsonArray precedingTitlesJson = new JsonArray().add(new JsonObject()
      .put(PrecedingSucceedingTitle.TITLE_KEY, "Butterflies in the snow"));

    Instance initialInstance = Instance.fromJson(new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put(Instance.TITLE_KEY, "Jewish life")
      .put(Instance.PRECEDING_TITLES_KEY, precedingTitlesJson));

    JsonObject precedingSucceedingTitles = new JsonObject().put(PRECEDING_SUCCEEDING_TITLES_KEY, precedingTitlesJson);
    when(mockedOkapiHttpClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(getOkResponse(precedingSucceedingTitles.encode())));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(initialInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(initialInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get();
    assertNotNull(eventPayload);
    Instance updatedInstance = Instance.fromJson(new JsonObject(eventPayload.getContext().get(INSTANCE.value())));
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertNotNull(initialInstance.getPrecedingTitles());
    assertEquals(initialInstance.getId(), updatedInstance.getId());
    assertTrue(updatedInstance.getPrecedingTitles().isEmpty());
  }

  @Test
  void shouldNotUpdateInstanceIf999ff$iFieldIsBlanks()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"}]}";
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(),
      Json.encode(marcRecord.withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField =
      "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    assertTrue(addedField.isPresent());
    assertEquals(expectedAddedField, addedField.get().encode());
    assertEquals(existingInstance.getId(), instanceJson.getString("id"));

    verify(mockedInstanceCollection, never()).update(any(), any(), any());
  }

  @Test
  void shouldNotUpdateInstanceIfOLErrorExist() {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(
        "Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1",
        409));
      return null;
    }).when(mockedInstanceCollection).update(any(), any(), any());

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldNotUpdateInstanceIfErrorDuringInstanceUpdate() {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Fail", 400));
      return null;
    }).when(mockedInstanceCollection).update(any(), any(), any());

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldFailIfErrorDuringRecordUpdate() {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    when(sourceStorageClient.putSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.failedFuture("Error"));

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldFailIfRecordUpdateReturnsNot200StatusCode() {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    when(putRecordHttpResponse.statusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnFailedFutureWhenHasNoMarcRecord() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    // then
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnTrueWhenHandlerIsEligibleForProfileAndEvent() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    assertTrue(isEligible);
  }

  @Test
  void shouldReturnFalseEligibleWhenActionProfileNotModify() {
    // given
    ActionProfile createInstanceProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileWrapper = new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withContent(createInstanceProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(profileWrapper);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    assertFalse(isEligible);
  }

  @Test
  void shouldReturnFalseEligibleWhenPayloadDoesNotContainProfileSnapshotWrapper() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(null);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    assertFalse(isEligible);
  }

  @Test
  void shouldClearExternalIdsHolderInstanceIdWhenDeleteProfileRemoves999Field()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given: MODIFY profile that DELETEs the whole 999 field
    MarcMappingDetail deleteDetail = new MarcMappingDetail()
      .withOrder(0)
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("999")
        .withIndicator1("*")
        .withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));

    MappingProfile deleteMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Delete 999")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC)
      .withMappingDetails(new MappingDetail()
        .withMarcMappingDetails(Collections.singletonList(deleteDetail))
        .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY));

    ProfileSnapshotWrapper deleteSnapshotWrapper = new ProfileSnapshotWrapper()
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(actionProfile).getMap())
      .withChildSnapshotWrappers(Collections.singletonList(
        new ProfileSnapshotWrapper()
          .withProfileId(deleteMappingProfile.getId())
          .withContentType(MAPPING_PROFILE)
          .withContent(JsonObject.mapFrom(deleteMappingProfile).getMap())));

    HashMap<String, String> payloadContext = new HashMap<>();
    // record fixture has externalIdsHolder.instanceId = ddd266ef-... and 999ff$i in parsed content
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(deleteSnapshotWrapper)
      .withTenant(TENANT_ID);

    // sanity precondition: holder is populated before handling
    Record recordBefore = Json.decodeValue(payloadContext.get(MARC_BIBLIOGRAPHIC.value()), Record.class);
    assertNotNull(recordBefore.getExternalIdsHolder());
    assertNotNull(recordBefore.getExternalIdsHolder().getInstanceId());

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload result = future.get(5, TimeUnit.SECONDS);

    // then
    Record actualRecord = Json.decodeValue(result.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
    Optional<JsonObject> field999 =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "999");
    assertFalse(field999.isPresent(), "999 field must be removed from parsed content");
    assertTrue(
      actualRecord.getExternalIdsHolder() == null
      || actualRecord.getExternalIdsHolder().getInstanceId() == null
      || actualRecord.getExternalIdsHolder().getInstanceId().isEmpty(),
      "externalIdsHolder.instanceId must be cleared after 999 removal");
  }

  @Test
  void shouldNotClearExternalIdsHolderInstanceIdWhenModifyProfileDoesNotRemove999Field()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given: default profile only ADDs 856 (does not touch 999)
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    String originalInstanceId = marcRecord.getExternalIdsHolder().getInstanceId();

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload result = future.get(5, TimeUnit.SECONDS);

    // then: 999 must remain, holder must remain untouched
    Record actualRecord = Json.decodeValue(result.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
    Optional<JsonObject> field999 =
      getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "999");
    assertTrue(field999.isPresent(), "999 field must still be present");
    assertNotNull(actualRecord.getExternalIdsHolder());
    assertEquals(originalInstanceId, actualRecord.getExternalIdsHolder().getInstanceId(),
      "externalIdsHolder.instanceId must remain unchanged");
  }

  static Optional<JsonObject> getFieldFromParsedRecord(String parsedContent, String field) {
    JsonObject parsedContentAsJson = new JsonObject(parsedContent);
    return parsedContentAsJson.getJsonArray("fields").stream().map(o -> (JsonObject) o)
      .filter(o -> o.containsKey(field)).findFirst();
  }

  private Response getOkResponse(String body) {
    return new Response(200, body, null, null);
  }
}
