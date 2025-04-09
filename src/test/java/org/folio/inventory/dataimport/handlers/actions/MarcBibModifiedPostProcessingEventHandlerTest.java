package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.MappingMetadataDto;
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
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
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
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MarcBibModifiedPostProcessingEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
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

  private JsonObject mappingRules;
  private Record record;
  private Instance existingInstance;
  private MarcBibModifiedPostProcessingEventHandler marcBibModifiedEventHandler;

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update item-SR")
    .withAction(MODIFY)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC bib")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
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

  @Before
  public void setUp() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

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

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(ctxt -> mockedOkapiHttpClient);
    marcBibModifiedEventHandler = new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(mockedStorage), precedingSucceedingTitlesHelper, mappingMetadataCache);
  }

  @Test
  public void shouldUpdateInstanceAtCentralTenantIfCentralTenantIdExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("CENTRAL_TENANT_ID", CENTRAL_TENANT_ID);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);

    // then
    Mockito.verify(mappingMetadataCache).get(eq(dataImportEventPayload.getJobExecutionId()), argThat(context -> context.getTenantId().equals(TENANT_ID)));
    Mockito.verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
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
  }

  @Test
  public void shouldUpdateInstance() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);

    // then
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
  }

  @Test
  public void shouldUpdateInstanceWithDiscoverySuppressedAndNotSetStaffSuppressed() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    Record discoverySuppressedRecord = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true));

    Instance deletedInstance = existingInstance.copyInstance().setDiscoverySuppress(true);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(deletedInstance));
      return null;
    }).when(mockedInstanceCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(discoverySuppressedRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Instance updatedInstance = Instance.fromJson(instanceJson);

    // then
    Assert.assertEquals(existingInstance.getId(), instanceJson.getString("id"));
    Assert.assertEquals("Victorian environmental nightmares and something else/", updatedInstance.getIndexTitle());
    Assert.assertNotNull(updatedInstance.getIdentifiers().stream().filter(i -> "(OCoLC)1060180367".equals(i.value)).findFirst().get());
    Assert.assertNotNull(updatedInstance.getContributors().stream().filter(c -> "Mazzeno, Laurence W., 1234566".equals(c.name)).findFirst().get());
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0dd", updatedInstance.getStatisticalCodeIds().get(0));
    Assert.assertEquals("b5968c9e-cddc-4576-99e3-8e60aed8b0cf", updatedInstance.getNatureOfContentTermIds().get(0));
    Assert.assertTrue(updatedInstance.getDiscoverySuppress());
    Assert.assertFalse(updatedInstance.getStaffSuppress());
    Assert.assertNotNull(updatedInstance.getSubjects());
    Assert.assertEquals(1, updatedInstance.getSubjects().size());
    assertThat(updatedInstance.getSubjects().get(0).getValue(), Matchers.containsString("additional subfield"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
  }


  @Test(expected = ExecutionException.class)
  public void shouldNotUpdateInstanceIfOLErrorExist() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(mockedInstanceCollection).update(any(), any(), any());

    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
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

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get();
    Assert.assertNotNull(eventPayload);
    Instance updatedInstance = Instance.fromJson(new JsonObject(eventPayload.getContext().get(INSTANCE.value())));
    Assert.assertNotNull(existingInstance.getPrecedingTitles());
    Assert.assertEquals(existingInstance.getId(), updatedInstance.getId());
    Assert.assertTrue(updatedInstance.getPrecedingTitles().isEmpty());
  }

  @Test
  public void shouldReturnCompletedFutureWhenParsedContentHasNoInstanceId() {
    //given
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent("{}"));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING")
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((payload, throwable) -> Assert.assertNull(throwable));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibModifiedEventHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForProfileAndEvent() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING")
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = marcBibModifiedEventHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
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
    boolean isEligible = marcBibModifiedEventHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }

  private Response getOkResponse(String body) {
    return new Response(200, body, null, null);
  }
}
