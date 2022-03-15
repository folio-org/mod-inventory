package org.folio.inventory.dataimport.handlers.actions;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class DeleteAuthorityEventHandlerTest {
  private static final String AUTHORITY_RECORD_ID = "AUTHORITY_RECORD_ID";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Delete Marc Authorities")
    .withAction(DELETE)
    .withFolioRecord(MARC_AUTHORITY);

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(actionProfile.getId())
    .withContentType(ACTION_PROFILE)
    .withContent(actionProfile);

  @Mock
  private AuthorityRecordCollection authorityCollection;

  @Mock
  private Storage storage;

  private DeleteAuthorityEventHandler eventHandler;

  private HashMap<String, String> context = new HashMap<>();

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    eventHandler = new DeleteAuthorityEventHandler(storage);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Void>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(authorityCollection).delete(any(), any(), any());

    context.put(AUTHORITY_RECORD_ID, UUID.randomUUID().toString());
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityCollection);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<org.folio.DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    org.folio.DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);

    assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(AUTHORITY_RECORD_ID));
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsNull() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIdIsEmptyInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put(AUTHORITY_RECORD_ID, "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIdIsNotInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put("Test_Value", "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);
    assertTrue(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfPayloadIsNull() {
    assertFalse(eventHandler.isEligible(null));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty(){
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile(){
    JobProfile jobProfile = new JobProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create MARC Authority")
      .withDataType(JobProfile.DataType.MARC);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotDelete() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Update Marc Authorities")
      .withAction(UPDATE)
      .withFolioRecord(MARC_AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);

    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotAuthority() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Delete Marc Authorities")
      .withAction(DELETE)
      .withFolioRecord(ITEM);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper);

    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

}
