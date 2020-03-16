package org.folio.inventory.dataimport.handlers.action;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.codehaus.jackson.map.ObjectMapper;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.Holdingsrecord;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.holding.HoldingsWriterFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.vertx.core.json.JsonObject;

public class CreateHoldingEventHandlerTest {

  @Mock
  private Storage storage;
  @Mock
  HoldingsRecordCollection holdingsRecordsCollection;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create preliminary Item")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(HOLDINGS);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withPath("permanentLocationId").withValue("permanentLocationExpression"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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
            .withContent(mappingProfile)))));

  @Before
  public void setUp(){
    MockitoAnnotations.initMocks(this);
    MappingManager.clearReaderFactories();
  }

  @Test
  public void shouldProcessEvent() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    Mockito.when(fakeReader.read(eq("permanentLocationExpression"))).thenReturn(StringValue.of(permanentLocationId));

    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Holdingsrecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Holdingsrecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingsWriterFactory());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(new Holdingsrecord())).encode());
    context.put("INSTANCE", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(instance)).encode());
    CreateHoldingEventHandler createHoldingEventHandler = new CreateHoldingEventHandler(storage);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_CREATED_HOLDINGS_RECORD")
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload dataImportEventPayload1 = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals("DI_CREATED_HOLDINGS_RECORD", dataImportEventPayload1.getEventType());
    Assert.assertNotNull(dataImportEventPayload1.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(instanceId, new JsonObject(dataImportEventPayload1.getContext().get(HOLDINGS.value())).getString("instanceId"));
    Assert.assertEquals(permanentLocationId, new JsonObject(dataImportEventPayload1.getContext().get(HOLDINGS.value())).getString("permanentLocationId"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfInstanceIdIsNotExistsInContext() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    Mockito.when(fakeReader.read(eq("permanentLocationExpression"))).thenReturn(StringValue.of(permanentLocationId));

    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Holdingsrecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Holdingsrecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingsWriterFactory());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(new Holdingsrecord())).encode());
    context.put("INSTANCE", new JsonObject().encode());
    CreateHoldingEventHandler createHoldingEventHandler = new CreateHoldingEventHandler(storage);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_CREATED_HOLDINGS_RECORD")
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload dataImportEventPayload1 = future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfPermanentLocationIdIsNotExistsInContext() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    Mockito.when(fakeReader.read(eq("permanentLocationExpression"))).thenReturn(StringValue.of(""));

    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> {
      Holdingsrecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Holdingsrecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).add(any(), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingsWriterFactory());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    context.put("HOLDINGS", new JsonObject(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(new Holdingsrecord())).encode());
    context.put("INSTANCE", new JsonObject().encode());
    CreateHoldingEventHandler createHoldingEventHandler = new CreateHoldingEventHandler(storage);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("DI_CREATED_HOLDINGS_RECORD")
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = createHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload dataImportEventPayload1 = future.get(5, TimeUnit.MILLISECONDS);
  }
}
