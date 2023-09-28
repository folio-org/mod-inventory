package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
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
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MarcBibMatchedPostProcessingEventHandlerTest {

  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String OKAPI_URL = "http://localhost";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";
  private static final String TOKEN = "dummy";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private HoldingsRecordCollection mockedHoldingsCollection;
  private Record record;
  private Instance existingInstance;
  private MarcBibMatchedPostProcessingEventHandler marcBibMatchedPostProcessingEventHandler;

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
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.getParsedRecord().withContent(JsonObject.mapFrom(record.getParsedRecord().getContent()).encode());

    when(mockedStorage.getInstanceCollection(any())).thenReturn(mockedInstanceCollection);
    when(mockedStorage.getHoldingsRecordCollection(any())).thenReturn(mockedHoldingsCollection);
    when(mockedInstanceCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(existingInstance));

    doAnswer(invocationOnMock -> {
      Consumer<Success<MultipleRecords<HoldingsRecord>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(new MultipleRecords<>(null, 0)));
      return null;
    }).when(mockedHoldingsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any());

    marcBibMatchedPostProcessingEventHandler = new MarcBibMatchedPostProcessingEventHandler(mockedStorage);
  }

  @Test
  public void shouldSetInstanceFromCentralTenantIfCentralTenantIdExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));
    payloadContext.put("CENTRAL_TENANT_ID", CENTRAL_TENANT_ID);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibMatchedPostProcessingEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Mockito.verify(mockedStorage).getInstanceCollection(argThat(context -> context.getTenantId().equals(CENTRAL_TENANT_ID)));
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Assert.assertNotNull(instanceJson);
    Instance updatedInstance = Instance.fromJson(instanceJson);
    Assert.assertEquals(updatedInstance.getId(), existingInstance.getId());
  }

  @Test
  public void shouldSetInstanceFromLocalTenant() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    InstanceCollection centralTenantInstanceCollection = Mockito.mock(InstanceCollection.class);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withOkapiUrl(OKAPI_URL)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = marcBibMatchedPostProcessingEventHandler.handle(dataImportEventPayload);

    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    JsonObject instanceJson = new JsonObject(eventPayload.getContext().get(INSTANCE.value()));
    Mockito.verify(centralTenantInstanceCollection, times(0)).findById(any());
    Assert.assertNotNull(instanceJson);
    Instance updatedInstance = Instance.fromJson(instanceJson);
    Assert.assertEquals(updatedInstance.getId(), existingInstance.getId());
  }
}
