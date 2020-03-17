package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.mapping.mapper.writer.item.ItemWriterFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

public class CreateItemEventHandlerTest {

  @Mock
  private Storage mockedStorage;
  @Mock
  private ItemCollection mockedItemCollection;
  @Mock
  private Reader fakeReader;
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
    .withFolioRecord(ActionProfile.FolioRecord.ITEM);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(ITEM)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Arrays.asList(
        new MappingRule().withPath("status.name").withValue("statusExpression"),
        new MappingRule().withPath("permanentLoanType.id").withValue("permanentLoanTypeExpression"),
        new MappingRule().withPath("materialType.id").withValue("materialTypeExpression"),
        new MappingRule().withPath("holdingsRecordId").withValue("holdingsRecordIdExpression"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private String marcRecord = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ { \"001\":\"ybp7406411\" } ] }";

  private CreateItemEventHandler createItemHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    Mockito.when(fakeReader.read(eq("statusExpression"))).thenReturn(StringValue.of(AVAILABLE.value()));
    Mockito.when(fakeReader.read(eq("permanentLoanTypeExpression"))).thenReturn(StringValue.of(UUID.randomUUID().toString()));
    Mockito.when(fakeReader.read(eq("materialTypeExpression"))).thenReturn(StringValue.of(UUID.randomUUID().toString()));
    Mockito.when(fakeReader.read(eq("holdingsRecordIdExpression"))).thenReturn(StringValue.of(UUID.randomUUID().toString()));
    Mockito.when(mockedStorage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedItemCollection);

    createItemHandler = new CreateItemEventHandler(mockedStorage);
    MappingManager.clearReaderFactories();
  }

  @Test
  public void shouldCreateItem()
    throws UnsupportedEncodingException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    Mockito.doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      Consumer<Success<Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(item));
      return null;
    }).when(mockedItemCollection).add(any(), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), marcRecord);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(CreateItemEventHandler.ITEM_CREATED_EVENT_TYPE, eventPayload.getEventType());
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonObject createdItem = new JsonObject(eventPayload.getContext().get(ITEM.value()));
    Assert.assertNotNull(createdItem.getJsonObject("status").getString("name"));
    Assert.assertNotNull(createdItem.getJsonObject("permanentLoanType").getString("id"));
    Assert.assertNotNull(createdItem.getJsonObject("materialType").getString("id"));
    Assert.assertNotNull(createdItem.getString("holdingsRecordId"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithoutStatus()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), marcRecord);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    Mockito.when(fakeReader.read(eq("statusExpression"))).thenReturn(StringValue.of(""));
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithUnrecognizedStatusName()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    CreateItemEventHandler createItemHandler = new CreateItemEventHandler(mockedStorage);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), marcRecord);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    Mockito.when(fakeReader.read(eq("statusExpression"))).thenReturn(StringValue.of("Invalid status"));
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenCreatedItemHasExistingBarcode()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), marcRecord);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithoutPermanentLoanType()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    // given
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), marcRecord);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    Mockito.when(fakeReader.read(eq("permanentLoanTypeExpression"))).thenReturn(StringValue.of(""));
    CompletableFuture<DataImportEventPayload> future = createItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = createItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }
}
