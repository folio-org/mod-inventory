package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;

public class UpdateAuthorityEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-authority-rules.json";
  private static final String PARSED_AUTHORITY_RECORD = "src/test/resources/marc/authority/parsed-authority-record.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

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
    .withIncomingRecordType(EntityType.MARC_AUTHORITY)
    .withExistingRecordType(EntityType.MARC_AUTHORITY)
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
  private AuthorityRecordCollection authorityCollection;

  @Mock
  private OkapiHttpClient mockedClient;

  @Mock
  private Storage storage;

  private UpdateAuthorityEventHandler eventHandler;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx, vertx.createHttpClient(), 3600);
    eventHandler = new UpdateAuthorityEventHandler(storage, mappingMetadataCache);
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Void>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(authorityCollection).update(any(), any(), any());

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityCollection);

    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_AUTHORITY_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(AUTHORITY.value()));
    assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(AUTHORITY.value())).getString("id"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsNull() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIsEmptyInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIsNotInContext()
    throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put("Test_Value", "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() throws IOException {
    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile));

    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage(), containsString("Unexpected payload"));
  }

  @Test
  public void isEligibleShouldReturnTrue() throws IOException {
    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(new HashMap<>());
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(AUTHORITY);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotAuthority() {
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
      .withEventType(DI_INVENTORY_AUTHORITY_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(eventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnFalse() {
    assertFalse(eventHandler.isPostProcessingNeeded());
  }
}
