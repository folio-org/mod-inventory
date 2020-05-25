package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.ItemWriterFactory;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.Storage;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

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

import static org.folio.ActionProfile.Action.REPLACE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class UpdateItemEventHandlerTest {

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
    .withName("Update item-SR")
    .withAction(REPLACE)
    .withFolioRecord(ActionProfile.FolioRecord.ITEM);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(ITEM)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Arrays.asList(
        new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.permanentLoanType.id").withValue("\"permanentLoanTypeExpression\"").withEnabled("true"),
        new MappingRule().withPath("item.materialType.id").withValue("\"materialTypeExpression\"").withEnabled("true"))));

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

  private Item oldItem;

  private UpdateItemEventHandler updateItemHandler;

  @Before
  public void setUp() {
    Mockito.doAnswer(invocationOnMock -> {
      Item item = invocationOnMock.getArgument(0);
      return CompletableFuture.completedFuture(item);
    }).when(mockedItemCollection).update(any(Item.class));

    Mockito.when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    Mockito.when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(IN_PROCESS.value()), StringValue.of(UUID.randomUUID().toString()), StringValue.of(UUID.randomUUID().toString()));
    Mockito.when(mockedStorage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(mockedItemCollection);

    MappingManager.clearReaderFactories();
    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new ItemWriterFactory());
    updateItemHandler = new UpdateItemEventHandler(mockedStorage);
    oldItem = new Item(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Status(AVAILABLE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null);
  }

  @Test
  public void shouldUpdateItemWithNewStatus()
    throws UnsupportedEncodingException, InterruptedException, ExecutionException, TimeoutException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      MultipleRecords<Item> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), Json.encode(oldItem));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    DataImportEventPayload eventPayload = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(DI_INVENTORY_ITEM_UPDATED, DataImportEventTypes.fromValue(eventPayload.getEventType()));
    Assert.assertNotNull(eventPayload.getContext().get(ITEM.value()));

    JsonObject updatedItem = new JsonObject(eventPayload.getContext().get(ITEM.value()));
    Assert.assertNotNull(updatedItem.getString("permanentLoanTypeId"));
    Assert.assertNotNull(updatedItem.getString("materialTypeId"));
    Assert.assertEquals(oldItem.id, updatedItem.getString("id"));
    Assert.assertEquals(oldItem.getHrid(), updatedItem.getString(HRID_KEY));
    Assert.assertEquals(oldItem.getHoldingId(), updatedItem.getString("holdingsRecordId"));
    Assert.assertEquals(IN_PROCESS.value(), updatedItem.getJsonObject(STATUS_KEY).getString("name"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithoutStatus()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    MappingRule statusMappingRule = new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true");
    Mockito.when(fakeReader.read(eq(statusMappingRule))).thenReturn(StringValue.of(""));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), Json.encode(oldItem));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenMappedItemWithUnrecognizedStatusName()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    MappingRule statusMappingRule = new MappingRule().withPath("item.status.name").withValue("\"statusExpression\"").withEnabled("true");
    Mockito.when(fakeReader.read(eq(statusMappingRule))).thenReturn(StringValue.of("Invalid status"));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), Json.encode(oldItem));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenBarcodeToUpdatedAssignedToAnotherItem()
    throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    // given
    Mockito.doAnswer(invocationOnMock -> {
      Item itemByCql = new Item(null, null, new Status(AVAILABLE), null, null, null);
      MultipleRecords<Item> result = new MultipleRecords<>(Collections.singletonList(itemByCql), 0);
      Consumer<Success<MultipleRecords<Item>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(mockedItemCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(ITEM.value(), Json.encode(oldItem));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoExistingItemToUpdate()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = updateItemHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = updateItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create preliminary Item")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.ITEM);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = updateItemHandler.isEligible(dataImportEventPayload);

    //then
    Assert.assertFalse(isEligible);
  }
}
