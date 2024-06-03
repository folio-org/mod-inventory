package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

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

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
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

@RunWith(MockitoJUnitRunner.class)
public class MarcBibModifyEventHandlerTest {
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";
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
    private Record record;
  private Instance existingInstance;
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

  private MarcBibModifyEventHandler marcBibModifyEventHandler;

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

  @Before
  public void setUp() throws IOException {
      JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

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

    Mockito.when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(mappingRules.encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    when(putRecordHttpResponse.statusCode()).thenReturn(HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(putRecordHttpResponse));

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(ctxt -> mockedOkapiHttpClient);
    marcBibModifyEventHandler = spy(new MarcBibModifyEventHandler(mappingMetadataCache, new InstanceUpdateDelegate(mockedStorage), precedingSucceedingTitlesHelper, httpClient));

    doReturn(sourceStorageClient).when(marcBibModifyEventHandler).getSourceStorageRecordsClient(any());
  }

  @Test
  public void shouldModifyRecordAndUpdateInstance() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField = "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

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
    Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField = getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertTrue(addedField.isPresent());
    Assert.assertEquals(expectedAddedField, addedField.get().encode());
    Assert.assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst().get());
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().get(0));
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().get(0));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);

    verify(mockedInstanceCollection).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(record.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString().equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  public void shouldModifyRecordAnNotUpdateInstanceIfEntityDoesNotExistAtContext() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    String expectedAddedField = "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

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
    Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField = getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertTrue(addedField.isPresent());
    Assert.assertEquals(expectedAddedField, addedField.get().encode());
    Assert.assertFalse(eventPayload.getContext().containsKey(INSTANCE.value()));

    verify(mockedInstanceCollection, never()).update(any(), any(), any());
    verify(sourceStorageClient, never()).putSourceStorageRecordsById(any(), any());
  }

  @Test
  public void shouldModifyMarcBibAndUpdateInstanceAtCentralTenantIfCentralTenantIdExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("CENTRAL_TENANT_ID", CENTRAL_TENANT_ID);
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField = "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

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

    Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField = getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    Assert.assertTrue(addedField.isPresent());
    Assert.assertEquals(expectedAddedField, addedField.get().encode());
    Mockito.verify(mappingMetadataCache).get(eq(dataImportEventPayload.getJobExecutionId()), argThat(context -> context.getTenantId().equals(TENANT_ID)));
    Mockito.verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst().get());
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().get(0));
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().get(0));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);

    verify(marcBibModifyEventHandler).getSourceStorageRecordsClient(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
    verify(mockedInstanceCollection).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(record.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString().equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  public void shouldModifyRecordAndUpdateInstanceAfterOptimisticLockingProcessing() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField = "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper)
      .withTenant(TENANT_ID);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
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
    Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField = getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    Assert.assertTrue(addedField.isPresent());
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(expectedAddedField, addedField.get().encode());
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst().get());
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().get(0));
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().get(0));
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);

    verify(mockedInstanceCollection, times(2)).update(any(), any(), any());
    verify(sourceStorageClient).putSourceStorageRecordsById(eq(record.getId()),
      argThat(r -> r.getParsedRecord().getContent().toString().equals(actualRecord.getParsedRecord().getContent().toString())));
  }

  @Test
  public void shouldRemovePrecedingTitlesOnInstanceUpdateWhenIncomingRecordHasNot() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    JsonArray precedingTitlesJson = new JsonArray().add(new JsonObject()
      .put(PrecedingSucceedingTitle.TITLE_KEY, "Butterflies in the snow"));

    Instance existingInstance = Instance.fromJson(new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put(Instance.TITLE_KEY, "Jewish life")
      .put(Instance.PRECEDING_TITLES_KEY, precedingTitlesJson));

    JsonObject precedingSucceedingTitles = new JsonObject().put(PRECEDING_SUCCEEDING_TITLES_KEY, precedingTitlesJson);
    when(mockedOkapiHttpClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(getOkResponse(precedingSucceedingTitles.encode())));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

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
    Assert.assertNotNull(eventPayload);
    Instance updatedInstance = Instance.fromJson(new JsonObject(eventPayload.getContext().get(INSTANCE.value())));
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertNotNull(existingInstance.getPrecedingTitles());
    Assert.assertEquals(existingInstance.getId(), updatedInstance.getId());
    Assert.assertTrue(updatedInstance.getPrecedingTitles().isEmpty());
  }

  @Test
  public void shouldNotUpdateInstanceIf999ff$iFieldIsBlanks() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"}]}";
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record.withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));
    String expectedAddedField = "{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=\"}],\"ind1\":\" \",\"ind2\":\" \"}}";

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
    Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);

    // then
    Optional<JsonObject> addedField = getFieldFromParsedRecord(actualRecord.getParsedRecord().getContent().toString(), "856");
    Assert.assertFalse(dataImportEventPayload.getContext().containsKey(CURRENT_RETRY_NUMBER));
    Assert.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), dataImportEventPayload.getEventType());
    Assert.assertEquals(MAPPING_PROFILE, dataImportEventPayload.getCurrentNode().getContentType());
    Assert.assertTrue(addedField.isPresent());
    Assert.assertEquals(expectedAddedField, addedField.get().encode());
    Assert.assertEquals(existingInstance.getId(), instanceJson.getString("id"));

    verify(mockedInstanceCollection, never()).update(any(), any(), any());
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotUpdateInstanceIfOLErrorExist() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(mockedInstanceCollection).update(any(), any(), any());

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotUpdateInstanceIfErrorDuringInstanceUpdate() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldFailIfErrorDuringRecordUpdate() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldFailIfRecordUpdateReturnsNot200StatusCode() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put(INSTANCE.value(), Json.encode(existingInstance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper);

    when(putRecordHttpResponse.statusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);

    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifyEventHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForProfileAndEvent() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseEligibleWhenActionProfileNotModify() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldReturnFalseEligibleWhenPayloadDoesNotContainProfileSnapshotWrapper() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INCOMING_MARC_BIB_RECORD_PARSED.value())
      .withCurrentNode(null);

    // when
    boolean isEligible = marcBibModifyEventHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }

  private Response getOkResponse(String body) {
    return new Response(200, body, null, null);
  }

  public static Optional<JsonObject> getFieldFromParsedRecord(String parsedContent, String field) {
    JsonObject parsedContentAsJson = new JsonObject(parsedContent);
    return parsedContentAsJson.getJsonArray("fields").stream().map(o -> (JsonObject) o)
      .filter(o -> o.containsKey(field)).findFirst();
  }
}
