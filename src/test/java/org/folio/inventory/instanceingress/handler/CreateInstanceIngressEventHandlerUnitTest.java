package org.folio.inventory.instanceingress.handler;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.INDICATOR_F;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_L;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_TYPE;
import static org.folio.rest.jaxrs.model.InstanceIngressPayload.SourceType.LINKED_DATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static support.TestUtil.buildHttpResponseWithBuffer;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.http.HttpStatus;
import org.folio.MappingMetadataDto;
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
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import support.TestUtil;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CreateInstanceIngressEventHandlerUnitTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String BIB_RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String TOKEN = "stub-token";
  private static final String OKAPI_URL = "https://example.com";
  private static final String TENANT = "test-tenant";
  private static final String USER_ID = "12456";

  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private SourceStorageSnapshotsClient sourceStorageSnapshotsClient;
  @Mock
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private IdStorageService idStorageService;
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

  private CreateInstanceIngressEventHandler handler;

  @BeforeEach
  void setUp() {
    doReturn(TENANT).when(context).getTenantId();
    doReturn(OKAPI_URL).when(context).getOkapiLocation();
    doReturn(TOKEN).when(context).getToken();
    doReturn(USER_ID).when(context).getUserId();
    doReturn(instanceCollection).when(storage).getInstanceCollection(context);
    handler = spy(new CreateInstanceIngressEventHandler(precedingSucceedingTitlesHelper,
      mappingMetadataCache, idStorageService, httpClient, context, storage, snapshotService));
  }

  @Test
  void shouldReturnFailedFuture_ifEventDoesNotContainData() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString());
    var expectedMessage = format("InstanceIngressEvent message does not contain " +
                                 "required data to create Instance for eventId: '%s'", event.getId());

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(expectedMessage, exception.getCause().getMessage());
  }

  @Test
  void shouldReturnFailedFuture_ifIdStorageServiceStoreFails() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    var expectedMessage = "idStorageService failure";
    doReturn(failedFuture(expectedMessage)).when(idStorageService).store(anyString(), anyString(), anyString());

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifMappingMetadataWasNotFound() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    doReturn(succeededFuture(Optional.empty())).when(mappingMetadataCache)
      .getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var expectedMessage = "MappingMetadata was not found for marc-bib record type";

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifInstanceValidationFails() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject("{}")
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);

    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());

    var expectedMessage = "Mapped Instance is invalid: [Field 'title' is a required field and can not be null, "
                          + "Field 'instanceTypeId' is a required field and can not be null], from InstanceIngressEvent with id '"
                          + event.getId() + "'";

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifInstanceSavingFailed() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());

    var expectedMessage = "Some failure";
    doAnswer(i -> {
      Consumer<Failure> failureHandler = i.getArgument(2);
      failureHandler.accept(new Failure(expectedMessage, 400));
      return null;
    }).when(instanceCollection).add(any(), any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifCreatePrecedingSucceedingTitlesFailed() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    var expectedMessage = "CreatePrecedingSucceedingTitles failure";
    doReturn(failedFuture(expectedMessage)).when(precedingSucceedingTitlesHelper)
      .createPrecedingSucceedingTitles(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifSourceStorageSnapshotsClientReturnsError() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(failedFuture("Failed to create snapshot in SRS, snapshot id: test-id"))
      .when(handler).postSnapshotInSrsAndHandleResponse(anyString(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    var snapshotHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());

    var expectedMessage = "Failed to create snapshot in SRS, snapshot id: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  void shouldReturnFailedFuture_ifItsFailedToCreateMarcRecordInSrs() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).postSourceStorageRecords(any());
    doReturn(succeededFuture()).when(handler).postSnapshotInSrsAndHandleResponse(anyString(), any(), any());

    var expectedMessage = "Failed to create MARC record in SRS, instanceId: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  void shouldReturnSucceededFuture_ifProcessFinishedCorrectly() throws ExecutionException, InterruptedException {
    // given
    var linkedDataId = "someLinkedDataId";
    var instanceId = "someInstanceId";
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(new InstanceIngressPayload()
        .withSourceRecordObject(TestUtil.readFileFromPath(BIB_RECORD_PATH))
        .withSourceType(LINKED_DATA)
        .withSourceRecordIdentifier(UUID.randomUUID().toString())
        .withAdditionalProperty("linkedDataId", linkedDataId)
        .withAdditionalProperty("instanceId", instanceId)
      );
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(metadataCacheKey(), context, MARC_BIB_RECORD_TYPE);
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient)
      .postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any(), any());
    var sourceStorageHttpResponse =
      buildHttpResponseWithBuffer(buffer(Json.encode(new Record())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).postSourceStorageRecords(any());
    doReturn(succeededFuture()).when(handler).postSnapshotInSrsAndHandleResponse(anyString(), any(), any());

    // when
    var future = handler.handle(event);

    // then
    assertThat(future.isDone()).isTrue();

    var instance = future.get();
    assertThat(instance.getId()).isEqualTo(instanceId);
    assertThat(instance.getSource()).isEqualTo("LINKED_DATA");
    assertThat(instance.getIdentifiers().stream().anyMatch(i -> i.value().equals("(ld) " + linkedDataId))).isTrue();
    verify(handler).getSourceStorageRecordsClient(any(), any(), argThat(TENANT::equals), argThat(USER_ID::equals),
      any());

    var recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    var recordSentToSRS = recordCaptor.getValue();
    assertThat(recordSentToSRS.getId()).isEqualTo(event.getEventPayload().getSourceRecordIdentifier());
    assertThat(recordSentToSRS.getRecordType()).isEqualTo(Record.RecordType.MARC_BIB);
    assertThat(
      AdditionalFieldsUtil.getValueFromDataField(recordSentToSRS, TAG_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I))
      .hasValue(instance.getId());
    assertThat(
      AdditionalFieldsUtil.getValueFromDataField(recordSentToSRS, TAG_999, INDICATOR_F, INDICATOR_F, SUBFIELD_L))
      .hasValue(linkedDataId);
  }

  private String metadataCacheKey() {
    return InstanceIngressEventConsumer.class.getSimpleName() + "-" + TENANT;
  }
}
