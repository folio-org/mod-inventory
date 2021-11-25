package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.CreateMarcAuthorityEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
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
import org.mockito.MockitoAnnotations;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

public class CreateMarcAuthorityEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-authority-rules.json";
  private static final String PARSED_AUTHORITY_RECORD = "src/test/resources/marc/authority/parsed-authority-record.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";

  @Mock
  private Storage storage;
  @Mock
  AuthorityRecordCollection authorityCollection;
  @Mock
  OkapiHttpClient mockedClient;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
      WireMockConfiguration.wireMockConfig()
          .dynamicPort()
          .notifier(new Slf4jNotifier(true)));

  private JsonObject mappingRules;
  private CreateMarcAuthorityEventHandler createMarcAuthoritiesEventHandler;
  private Vertx vertx = Vertx.vertx();

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Authority")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Authority")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(AUTHORITY);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("preliminary Authority from MARC")
    .withIncomingRecordType(EntityType.MARC_AUTHORITY)
    .withExistingRecordType(EntityType.AUTHORITY)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression").withEnabled("true"))));

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

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = new MappingMetadataCache(vertx, vertx.createHttpClient(), 3600);
    createMarcAuthoritiesEventHandler = new CreateMarcAuthorityEventHandler(storage, mappingMetadataCache);
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      Authority authority = invocationOnMock.getArgument(0);
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(authority));
      return null;
    }).when(authorityCollection).add(any(), any(Consumer.class), any(Consumer.class));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));

    doAnswer(invocationOnMock -> completedStage(new Response(HttpStatus.SC_CREATED, null, null, null)))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityCollection);

    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withOkapiUrl(mockServer.baseUrl())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_AUTHORITY_CREATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(AUTHORITY.value()));
    Assert.assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(AUTHORITY.value())).getString("id"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsNull() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfContextIsEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(new HashMap<>() )
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIsEmptyInContext() throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldThrowExceptionIfMarcAuthorityIsNotInContext() throws ExecutionException, InterruptedException, TimeoutException {

    HashMap<String, String> context = new HashMap<>();
    context.put("Test_Value", "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() throws IOException {

    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() throws IOException {
    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withProfileId(jobProfile.getId())
        .withContentType(JOB_PROFILE)
        .withContent(jobProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
        .withContext(new HashMap<>())
        .withCurrentNode(profileSnapshotWrapper);
    assertFalse(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
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
        .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
        .withContext(new HashMap<>())
        .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
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
        .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
        .withContext(new HashMap<>())
        .withProfileSnapshot(profileSnapshotWrapper);
    assertFalse(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnFalse() {
    assertFalse(createMarcAuthoritiesEventHandler.isPostProcessingNeeded());
  }
}
