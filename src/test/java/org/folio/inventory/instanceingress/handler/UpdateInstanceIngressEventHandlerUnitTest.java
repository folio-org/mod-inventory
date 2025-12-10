package org.folio.inventory.instanceingress.handler;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.TestUtil.buildHttpResponseWithBuffer;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_L;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.rest.jaxrs.model.InstanceIngressPayload.SourceType.LINKED_DATA;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.http.HttpStatus;
import org.folio.MappingMetadataDto;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.storage.Storage;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(VertxUnitRunner.class)
public class UpdateInstanceIngressEventHandlerUnitTest {
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String BIB_RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  public static final String TENANT = "tenant";
  public static final String OKAPI_URL = "okapiUrl";
  public static final String TOKEN = "token";
  public static final String USER_ID = "userId";
  public static final String REQUEST_ID = "requestId";

  @Rule
  public MockitoRule initRule = MockitoJUnit.rule();
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private SourceStorageSnapshotsClient sourceStorageSnapshotsClient;
  @Mock
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private HttpClient httpClient;
  @Mock
  private Context context;
  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceCollection;
  @Mock
  private SnapshotService snapshotService;
  private UpdateInstanceIngressEventHandler handler;

  @Before
  public void setUp() {
    doReturn(TENANT).when(context).getTenantId();
    doReturn(OKAPI_URL).when(context).getOkapiLocation();
    doReturn(TOKEN).when(context).getToken();
    doReturn(USER_ID).when(context).getUserId();
    doReturn(instanceCollection).when(storage).getInstanceCollection(context);
    handler = spy(new UpdateInstanceIngressEventHandler(precedingSucceedingTitlesHelper,
      mappingMetadataCache, httpClient, context, storage, snapshotService));
  }

  @Test
  public void shouldReturnFailedFuture_ifEventDoesNotContainData() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString());
    var expectedMessage = format("InstanceIngressEvent message does not contain " +
      "required data to update Instance for eventId: '%s'", event.getId());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertEquals(expectedMessage, exception.getCause().getMessage());
  }

  @Test
  public void shouldReturnFailedFuture_ifMappingMetadataWasNotFound() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(Optional.empty())).when(mappingMetadataCache)
      .getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var expectedMessage = "MappingMetadata was not found for marc-bib record type";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifExistingInstanceFetchingFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var expectedMessage = "Error retrieving inventory Instance";
    doAnswer(i -> {
      Consumer<Failure> failureHandler = i.getArgument(2);
      failureHandler.accept(new Failure(expectedMessage, 400));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifInstanceValidationFails() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    var expectedMessage = "Mapped Instance is invalid: [Field 'title' is a required field and can not be null, "
      + "Field 'instanceTypeId' is a required field and can not be null], from InstanceIngressEvent with id '" + event.getId() + "'";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifInstanceUpdateFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    var expectedMessage = "Some failure";
    doAnswer(i -> {
      Consumer<Failure> failureHandler = i.getArgument(2);
      failureHandler.accept(new Failure(expectedMessage, 400));
      return null;
    }).when(instanceCollection).update(any(), any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifGetExistedPrecedingSucceedingTitlesFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var expectedMessage = "GetExistedPrecedingSucceedingTitlesFailed failure";
    doReturn(failedFuture(expectedMessage)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifDeletePrecedingSucceedingTitlesFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    var expectedMessage = "DeletePrecedingSucceedingTitlesFailed failure";
    doReturn(failedFuture(expectedMessage)).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifPostSourceStorageSnapshotFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());

    var expectedMessage = "Failed to create snapshot in SRS, snapshot id: ";
    doReturn(failedFuture(new EventProcessingException(expectedMessage)))
      .when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifItsFailedToGetRecordByInstanceIdFromSrsAndFailedToPutNewRecord() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    var snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());
    doReturn(succeededFuture(snapshot)).when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).getSourceStorageRecordsFormattedById(any(), any());
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), any());

    var expectedMessage = "Failed to update MARC record in SRS, instanceId: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifItsFailedToPutNewRecordToSRS() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    var snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());
    doReturn(succeededFuture(snapshot)).when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());
    var existedRecordResponse = buildHttpResponseWithBuffer(BufferImpl.buffer("{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\"}"), HttpStatus.SC_OK);
    doReturn(succeededFuture(existedRecordResponse)).when(sourceStorageClient).getSourceStorageRecordsFormattedById(any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), any());

    var expectedMessage = "Failed to update MARC record in SRS, instanceId: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifCreatePrecedingSucceedingTitlesFailed() throws IOException {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), any(), any());
    var snapshotHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());
    var existedRecordResponse = buildHttpResponseWithBuffer(BufferImpl.buffer("{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\"}"), HttpStatus.SC_OK);
    doReturn(succeededFuture(existedRecordResponse)).when(sourceStorageClient).getSourceStorageRecordsFormattedById(any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_OK);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), any());
    var expectedMessage = "CreatePrecedingSucceedingTitles failure";
    doReturn(failedFuture(expectedMessage)).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());

    var snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());
    doReturn(succeededFuture(snapshot)).when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnSucceededFuture_ifProcessFinishedCorrectly() throws IOException, ExecutionException, InterruptedException {
    // given
    var linkedDataId = "someLinkedDataId";
    var instanceId = "instanceId";
    var initialSrsId = UUID.randomUUID().toString();
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
        .withSourceRecordIdentifier(initialSrsId)
        .withAdditionalProperty("linkedDataId", linkedDataId)
        .withAdditionalProperty("instanceId", instanceId)
      );
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
   var existedInstance = new Instance(event.getId(), 1,
      UUID.randomUUID().toString(), null, null, null);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(existedInstance));
      return null;
    }).when(instanceCollection).findById(any(), any(), any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).update(any(), any(), any());
    var titles = List.of(JsonObject.of("id", "123"));
    doReturn(succeededFuture(titles)).when(precedingSucceedingTitlesHelper).getExistingPrecedingSucceedingTitles(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    var existedRecordResponse = buildHttpResponseWithBuffer(BufferImpl.buffer("{\"matchedId\":\"" + initialSrsId + "\"}"), HttpStatus.SC_OK);
    doReturn(succeededFuture(existedRecordResponse)).when(sourceStorageClient).getSourceStorageRecordsFormattedById(any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_OK);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());

    var snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString());
    doReturn(succeededFuture(snapshot)).when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    // when
    var future = handler.handle(event);

    // then
    assertThat(future.isDone()).isTrue();

    var instance = future.get();
    assertThat(instance.getId()).isEqualTo(instanceId);
    assertThat(instance.getHrid()).isEqualTo(existedInstance.getHrid());
    assertThat(instance.getSource()).isEqualTo("LINKED_DATA");
    assertThat(instance.getIdentifiers().stream().anyMatch(i -> i.value.equals("(ld) " + linkedDataId))).isTrue();

    var recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), recordCaptor.capture());
    verify(handler).getSourceStorageRecordsClient(any(), any(), argThat(TENANT::equals), argThat(USER_ID::equals), any());

    var snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
    var contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(snapshotService).postSnapshotInSrsAndHandleResponse(contextCaptor.capture(), snapshotCaptor.capture());
    assertEquals(TENANT, contextCaptor.getValue().getTenantId());
    assertEquals(USER_ID, contextCaptor.getValue().getUserId());

    var recordSentToSRS = recordCaptor.getValue();
    assertThat(recordSentToSRS.getId()).isNotNull();
    assertThat(recordSentToSRS.getId()).isNotEqualTo(initialSrsId);
    assertThat(recordSentToSRS.getMatchedId()).isEqualTo(initialSrsId);
    assertThat(recordSentToSRS.getRecordType()).isEqualTo(Record.RecordType.MARC_BIB);
    assertThat(AdditionalFieldsUtil.getValue(recordSentToSRS, TAG_999, SUBFIELD_I)).hasValue(instance.getId());
    assertThat(AdditionalFieldsUtil.getValue(recordSentToSRS, TAG_999, SUBFIELD_L)).hasValue(linkedDataId);
    assertThat(AdditionalFieldsUtil.getValue(recordSentToSRS, TAG_999, 's')).hasValue(initialSrsId);
  }

  private String metadataCacheKey() {
    return InstanceIngressEventConsumer.class.getSimpleName() + "-" + TENANT;
  }
}
