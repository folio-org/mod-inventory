package org.folio.inventory.instanceingress.handler;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.buffer.impl.BufferImpl.buffer;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
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
public class CreateInstanceIngressEventHandlerUnitTest {
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String BIB_RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  public static final String TOKEN = "token";
  public static final String OKAPI_URL = "okapiUrl";
  public static final String TENANT = "tenant";
  public static final String USER_ID = "userId";

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
  private IdStorageService idStorageService;
  @Mock
  private HttpClient httpClient;
  @Mock
  private Context context;
  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceCollection;
  private CreateInstanceIngressEventHandler handler;

  @Before
  public void setUp() {
    doReturn(TENANT).when(context).getTenantId();
    doReturn(OKAPI_URL).when(context).getOkapiLocation();
    doReturn(TOKEN).when(context).getToken();
    doReturn(USER_ID).when(context).getUserId();
    doReturn(instanceCollection).when(storage).getInstanceCollection(context);
    handler = spy(new CreateInstanceIngressEventHandler(precedingSucceedingTitlesHelper,
      mappingMetadataCache, idStorageService, httpClient, context, storage));
  }

  @Test
  public void shouldReturnFailedFuture_ifEventDoesNotContainData() {
    // given
    var event = new InstanceIngressEvent()
      .withId(UUID.randomUUID().toString());
    var expectedMessage = format("InstanceIngressEvent message does not contain " +
      "required data to create Instance for eventId: '%s'", event.getId());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertEquals(expectedMessage, exception.getCause().getMessage());
  }

  @Test
  public void shouldReturnFailedFuture_ifIdStorageServiceStoreFails() {
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
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
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
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    doReturn(succeededFuture(Optional.empty())).when(mappingMetadataCache)
      .getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    var expectedMessage = "MappingMetadata was not found for marc-bib record type";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
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
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);

    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());

    var expectedMessage = "Mapped Instance is invalid: [Field 'title' is a required field and can not be null, "
      + "Field 'instanceTypeId' is a required field and can not be null], from InstanceIngressEvent with id '" + event.getId() + "'";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifInstanceSavingFailed() throws IOException {
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
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());

    var expectedMessage = "Some failure";
    doAnswer(i -> {
      Consumer<Failure> failureHandler = i.getArgument(2);
      failureHandler.accept(new Failure(expectedMessage, 400));
      return null;
    }).when(instanceCollection).add(any(), any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
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
    doReturn(succeededFuture(null)).when(idStorageService).store(anyString(), anyString(), anyString());
    var mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    doReturn(succeededFuture(Optional.of(new MappingMetadataDto()
      .withMappingRules(mappingRules.encode())
      .withMappingParams(Json.encode(new MappingParameters())))))
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    var expectedMessage = "CreatePrecedingSucceedingTitles failure";
    doReturn(failedFuture(expectedMessage)).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).isEqualTo(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifSourceStorageSnapshotsClientReturnsError() throws IOException {
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
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    var snapshotHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());

    var expectedMessage = "Failed to create snapshot in SRS, snapshot id: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnFailedFuture_ifItsFailedToCreateMarcRecordInSrs() throws IOException {
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
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_BAD_REQUEST);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).postSourceStorageRecords(any());

    var expectedMessage = "Failed to create MARC record in SRS, instanceId: ";

    // when
    var future = handler.handle(event);

    // then
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    assertThat(exception.getCause().getMessage()).startsWith(expectedMessage);
  }

  @Test
  public void shouldReturnSucceededFuture_ifProcessFinishedCorrectly() throws IOException, ExecutionException, InterruptedException {
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
      .when(mappingMetadataCache).getByRecordType(InstanceIngressEventConsumer.class.getSimpleName(), context, MARC_BIB_RECORD_TYPE);
    doReturn(sourceStorageSnapshotsClient).when(handler).getSourceStorageSnapshotsClient(any(), any(), any(), argThat(USER_ID::equals));
    var snapshotHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(snapshotHttpResponse)).when(sourceStorageSnapshotsClient).postSourceStorageSnapshots(any());
    doAnswer(i -> {
      Consumer<Success<Instance>> successHandler = i.getArgument(1);
      successHandler.accept(new Success<>(i.getArgument(0)));
      return null;
    }).when(instanceCollection).add(any(), any(), any());
    doReturn(succeededFuture()).when(precedingSucceedingTitlesHelper).createPrecedingSucceedingTitles(any(), any());
    doReturn(sourceStorageClient).when(handler).getSourceStorageRecordsClient(any(), any(), any(), any());
    var sourceStorageHttpResponse = buildHttpResponseWithBuffer(buffer(Json.encode(new Record())), HttpStatus.SC_CREATED);
    doReturn(succeededFuture(sourceStorageHttpResponse)).when(sourceStorageClient).postSourceStorageRecords(any());

    // when
    var future = handler.handle(event);

    // then
    assertThat(future.isDone()).isTrue();

    var instance = future.get();
    assertThat(instance.getId()).isEqualTo(instanceId);
    assertThat(instance.getSource()).isEqualTo("LINKED_DATA");
    assertThat(instance.getIdentifiers().stream().anyMatch(i -> i.value.equals("(ld) " + linkedDataId))).isTrue();
    verify(handler).getSourceStorageRecordsClient(any(), any(), argThat(TENANT::equals), argThat(USER_ID::equals));

    var recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    var recordSentToSRS = recordCaptor.getValue();
    assertThat(recordSentToSRS.getId()).isEqualTo(event.getEventPayload().getSourceRecordIdentifier());
    assertThat(recordSentToSRS.getRecordType()).isEqualTo(Record.RecordType.MARC_BIB);
    assertThat(AdditionalFieldsUtil.getValue(recordSentToSRS, TAG_999, SUBFIELD_I)).hasValue(instance.getId());
    assertThat(AdditionalFieldsUtil.getValue(recordSentToSRS, TAG_999, SUBFIELD_L)).hasValue(linkedDataId);
  }
}
