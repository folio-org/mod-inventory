package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.inventory.dataimport.handlers.actions.CreateAuthorityEventHandler.ID_UNIQUENESS_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

import io.vertx.core.Future;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

@RunWith(VertxUnitRunner.class)
public class CreateAuthorityEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/marc-authority-rules.json";
  private static final String PARSED_AUTHORITY_RECORD = "src/test/resources/marc/authority/parsed-authority-record.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String AUTHORITY_ID = UUID.randomUUID().toString();
  private static final String RECORD_ID = UUID.randomUUID().toString();

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

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

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Mock
  private AuthorityRecordCollection authorityCollection;

  @Mock
  private OkapiHttpClient mockedClient;

  @Mock
  private Storage storage;

  @Mock
  private IdStorageService authorityIdStorageService;

  private CreateAuthorityEventHandler createMarcAuthoritiesEventHandler;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();
    MappingMetadataCache mappingMetadataCache = MappingMetadataCache.getInstance(rule.vertx(), rule.vertx().createHttpClient(), true);
    createMarcAuthoritiesEventHandler = new CreateAuthorityEventHandler(storage, mappingMetadataCache, authorityIdStorageService);
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    doAnswer(invocationOnMock -> {
      Authority authority = invocationOnMock.getArgument(0);
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(authority));
      return null;
    }).when(authorityCollection).add(any(), any(), any());

    doAnswer(invocationOnMock -> {
      RecordToEntity recordToItem = RecordToEntity.builder().recordId(RECORD_ID).entityId(AUTHORITY_ID).build();
      return Future.succeededFuture(recordToItem);
    }).when(authorityIdStorageService).store(any(), any(), any());

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.encode())))));

    doAnswer(invocationOnMock -> completedStage(new Response(HttpStatus.SC_CREATED, null, null, null)))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
  }

  @Test
  public void shouldProcessEvent(TestContext testContext) throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Async async = testContext.async();
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
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if(ar.failed()){
          testContext.fail(ar.cause());
        }
        DataImportEventPayload actualDataImportEventPayload = ar.result();
        testContext.assertNotNull(actualDataImportEventPayload);
        testContext.assertEquals(DI_INVENTORY_AUTHORITY_CREATED.value(), actualDataImportEventPayload.getEventType());
        testContext.assertNotNull(actualDataImportEventPayload.getContext().get(AUTHORITY.value()));
        testContext.assertNotNull(new JsonObject(actualDataImportEventPayload.getContext().get(AUTHORITY.value())).getString("id"));
        async.complete();
      });
  }

  @Test
  public void shouldNotProcessEventEvenIfDuplicatedInventoryStorageErrorExists(TestContext testContext) throws IOException {
    Async async = testContext.async();
    Errors errorResponse = new Errors().withErrors(List.of(new Error()
      .withMessage(ID_UNIQUENESS_ERROR)
      .withType("validation")
      .withCode("109")));

    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(Json.encode(errorResponse), 422));
      return null;
    }).when(authorityCollection).add(any(), any(), any());

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
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof DuplicateEventException);
          async.complete();
        }
      });
  }

  @Test
  public void shouldThrowExceptionIfContextIsNull(TestContext testContext) {
    Async async = testContext.async();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof EventProcessingException);
          async.complete();
        }
      });
  }

  @Test
  public void shouldThrowExceptionIfContextIsEmpty(TestContext testContext) {
    Async async = testContext.async();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof EventProcessingException);
          async.complete();
        }
      });
  }

  @Test
  public void shouldThrowExceptionIfMarcAuthorityIsEmptyInContext(TestContext testContext) {
    Async async = testContext.async();
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof EventProcessingException);
          async.complete();
        }
      });
  }

  @Test
  public void shouldThrowExceptionIfMarcAuthorityIsNotInContext(TestContext testContext) {
    Async async = testContext.async();
    HashMap<String, String> context = new HashMap<>();
    context.put("Test_Value", "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof EventProcessingException);
          async.complete();
        }
      });
  }

  @Test
  public void shouldNotProcessEventWhenRecordToAuthorityFutureFails(TestContext testContext) throws ExecutionException, InterruptedException, TimeoutException {
    Async async = testContext.async();
    when(authorityIdStorageService.store(any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Something wrong with database!")));

    String expectedHoldingId = UUID.randomUUID().toString();
    JsonObject holdingAsJson = new JsonObject().put("id", expectedHoldingId);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(EntityType.HOLDINGS.value(), holdingAsJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createMarcAuthoritiesEventHandler.handle(dataImportEventPayload);
    Future.fromCompletionStage(future)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          testContext.fail("Exception should be thrown");
        } else {
          testContext.assertTrue(ar.cause() instanceof EventProcessingException);
          async.complete();
        }
      });
  }

  @Test
  public void isEligibleShouldReturnTrue(TestContext testContext) throws IOException {
    var parsedAuthorityRecord = new JsonObject(TestUtil.readFileFromPath(PARSED_AUTHORITY_RECORD));

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_AUTHORITY.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(parsedAuthorityRecord.encode()))));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty(TestContext testContext) {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(createMarcAuthoritiesEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile(TestContext testContext) {
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
  public void isEligibleShouldReturnFalseIfRecordIsNotAuthority(TestContext testContext) {
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
  public void isPostProcessingNeededShouldReturnTrue(TestContext testContext) {
    assertTrue(createMarcAuthoritiesEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType(TestContext testContext) {
    assertEquals(DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING.value(), createMarcAuthoritiesEventHandler.getPostProcessingInitializationEventType());
  }
}
