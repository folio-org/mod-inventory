package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MarcBibModifiedPostProcessingEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String OKAPI_URL = "http://localhost";

  @Mock
  private Storage mockedStorage;
  @Mock
  InstanceCollection mockedInstanceCollection;
  @Mock
  OkapiHttpClient mockedOkapiHttpClient;

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

    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(ctxt -> mockedOkapiHttpClient);
    marcBibModifiedEventHandler = new MarcBibModifiedPostProcessingEventHandler(new InstanceUpdateDelegate(mockedStorage), precedingSucceedingTitlesHelper);
  }

  @Test
  public void shouldUpdateInstance() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("MAPPING_RULES", mappingRules.encode());
    payloadContext.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withProfileSnapshot(profileSnapshotWrapper)
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
    Assert.assertTrue(updatedInstance.getSubjects().get(0).contains("additional subfield"));
    Assert.assertFalse(updatedInstance.getSubjects().get(0).contains("Environmentalism in literature"));
    Assert.assertNotNull(updatedInstance.getNotes());
    Assert.assertEquals("Adding a note", updatedInstance.getNotes().get(0).note);
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
    payloadContext.put("MAPPING_RULES", mappingRules.encode());
    payloadContext.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value())
      .withContext(payloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withProfileSnapshot(profileSnapshotWrapper)
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
    payloadContext.put("MAPPING_RULES", mappingRules.encode());
    payloadContext.put("MAPPING_PARAMS", new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING")
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
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
      .withProfileSnapshot(profileSnapshotWrapper)
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
      .withProfileSnapshot(profileSnapshotWrapper)
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
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
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
