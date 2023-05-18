package org.folio.inventory.dataimport.handlers.actions;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.ItemUtil;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateHoldingEventHandlerTest {

  private static final String PARSED_CONTENT_WITH_INSTANCE_ID = "{ \"leader\": \"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"ybp7406411\"}, {\"999\": {\"ind1\":\"f\", \"ind2\":\"f\", \"subfields\":[ { \"i\": \"957985c6-97e3-4038-b0e7-343ecd0b8120\"} ] } } ] }";
  private static final String PARSED_CONTENT_WITH_INSTANCE_ID_AND_MULTIPLE_HOLDINGS = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"945\":{\"ind1\":\"\",\"ind2\":\"\",\"subfields\":[{\"h\":\"Online\"}]}},{\"945\":{\"ind1\":\"\",\"ind2\":\"\",\"subfields\":[{\"h\":\"Online 2\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"i\":\"957985c6-97e3-4038-b0e7-343ecd0b8120\"}]}}]}";

  private static final String ERRORS = "ERRORS";

  private static final String permanentLocationId = UUID.randomUUID().toString();

  @Mock
  private Storage storage;
  @Mock
  HoldingsRecordCollection holdingsRecordsCollection;
  @Mock
  ItemCollection itemCollection;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update preliminary Holdings")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(HOLDINGS);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withName("permanentLocationId").withPath("holdings.permanentLocationId[]").withValue("\"\\\"Main Library\\\"\"").withEnabled("true"))));

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
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private JobProfile jobProfileForMultipleHoldings = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfileForMultipleHoldings = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update preliminary Holdings")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(HOLDINGS);

  private MappingProfile mappingProfileForMultipleHoldings = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.HOLDINGS)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Collections.singletonList(
        new MappingRule().withName("permanentLocationId").withPath("holdings.permanentLocationId").withValue("945$h").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapperForMultipleHoldings = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfileForMultipleHoldings.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfileForMultipleHoldings)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfileForMultipleHoldings.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfileForMultipleHoldings)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfileForMultipleHoldings.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfileForMultipleHoldings).getMap())))));

  private UpdateHoldingEventHandler updateHoldingEventHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    MappingManager.clearReaderFactories();
    updateHoldingEventHandler = new UpdateHoldingEventHandler(storage, mappingMetadataCache);

    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordsCollection).update(any(), any(Consumer.class), any(Consumer.class));

    when(mappingMetadataCache.get(anyString(), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(new JsonObject().encode())
        .withMappingParams(Json.encode(new MappingParameters())))));
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String permanentLocationId = "a1e7c35d-4835-430a-ba3c-f93d5f3cde5a";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    String instanceId = UUID.randomUUID().toString();
    String holdingId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();


    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withInstanceId(instanceId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID_AND_MULTIPLE_HOLDINGS));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(resultedHoldingsList.size(), 1);
    JsonObject resultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(resultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, resultedHoldings.getString("instanceId"));
    Assert.assertEquals(permanentLocationId, resultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(firstHrid, resultedHoldings.getString("hrid"));
    Assert.assertEquals(holdingId, resultedHoldings.getString("id"));
  }

  @Test
  public void shouldProcessHoldingAndInstanceEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);


    JsonObject existingItemJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    doAnswer(invocationOnMock -> {
      Consumer<Success<org.folio.inventory.domain.items.Item>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(ItemUtil.jsonToItem(existingItemJson)));
      return null;
    }).when(itemCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));
    when(storage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    String instanceId = UUID.randomUUID().toString();
    String holdingId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();


    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withInstanceId(instanceId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(ITEM.value(), Json.encode(Lists.newArrayList(new JsonObject().put("item", existingItemJson))));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(resultedHoldingsList.size(), 1);
    JsonObject resultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(resultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, resultedHoldings.getString("instanceId"));
    Assert.assertEquals(permanentLocationId, resultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(firstHrid, resultedHoldings.getString("hrid"));
    Assert.assertEquals(holdingId, resultedHoldings.getString("id"));
  }

  @Test
  public void shouldProcessEventAndUpdateMultipleHoldings() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();
    List<String> locations = List.of(firstPermanentLocationId, secondPermanentLocationId);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(locations.get(0)), StringValue.of(locations.get(1)));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    String instanceId = UUID.randomUUID().toString();
    String holdingId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();


    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withInstanceId(instanceId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID_AND_MULTIPLE_HOLDINGS));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(resultedHoldingsList.size(), 2);
    JsonObject firstResultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(firstResultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, firstResultedHoldings.getString("instanceId"));
    Assert.assertEquals(firstPermanentLocationId, firstResultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(firstHrid, firstResultedHoldings.getString("hrid"));
    Assert.assertEquals(holdingId, firstResultedHoldings.getString("id"));
    JsonObject secondResultedHoldings = resultedHoldingsList.getJsonObject(1);
    Assert.assertNotNull(secondResultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, secondResultedHoldings.getString("instanceId"));
    Assert.assertEquals(secondPermanentLocationId, secondResultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(secondHrid, secondResultedHoldings.getString("hrid"));
    Assert.assertEquals(secondId, secondResultedHoldings.getString("id"));
  }

  @Test
  public void shouldProcessHoldingAndItemEventButWithPartialErrorIfItemUpdateFailed() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();
    List<String> locations = List.of(firstPermanentLocationId, secondPermanentLocationId);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(locations.get(0)), StringValue.of(locations.get(1)));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    String itemId = UUID.randomUUID().toString();
    JsonObject existingItemJson = new JsonObject()
      .put("id", itemId)
      .put("status", new JsonObject().put("name", AVAILABLE.value()))
      .put("materialType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("permanentLoanType", new JsonObject().put("id", UUID.randomUUID().toString()))
      .put("holdingsRecordId", UUID.randomUUID().toString());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());

    String instanceId = UUID.randomUUID().toString();
    String firstId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();

    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(firstId)
      .withInstanceId(instanceId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withInstanceId(instanceId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(ITEM.value(), Json.encode(Lists.newArrayList(new JsonObject().put("item", existingItemJson))));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(itemCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));
    when(storage.getItemCollection(ArgumentMatchers.any(Context.class))).thenReturn(itemCollection);

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(resultedHoldingsList.size(), 2);
    JsonObject firstResultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(firstResultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, firstResultedHoldings.getString("instanceId"));
    Assert.assertEquals(firstPermanentLocationId, firstResultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(firstHrid, firstResultedHoldings.getString("hrid"));
    Assert.assertEquals(firstId, firstResultedHoldings.getString("id"));
    JsonObject secondResultedHoldings = resultedHoldingsList.getJsonObject(1);
    Assert.assertNotNull(secondResultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, secondResultedHoldings.getString("instanceId"));
    Assert.assertEquals(secondPermanentLocationId, secondResultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(secondHrid, secondResultedHoldings.getString("hrid"));
    Assert.assertEquals(secondId, secondResultedHoldings.getString("id"));

    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, errors.size());
    JsonObject partialError = errors.getJsonObject(0);
    Assert.assertEquals("Internal Server Error", partialError.getString("error"));
    Assert.assertEquals(itemId, partialError.getString("id"));

  }

/*  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfOLErrorExist() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    String instanceId = String.valueOf(UUID.randomUUID());
    String holdingId = UUID.randomUUID().toString();
    String hrid = UUID.randomUUID().toString();

    HoldingsRecord holdingsRecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(hrid)
      .withPermanentLocationId(permanentLocationId);


    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    HoldingsRecord returnedHoldings = new HoldingsRecord().withId(holdingId).withHrid(hrid).withInstanceId(instanceId).withPermanentLocationId(permanentLocationId).withVersion(1);

    when(holdingsRecordsCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(returnedHoldings));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(holdingsRecordsCollection).update(any(), any(), any());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsRecord));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }*/

  @Test
  public void shouldNotProcessEventIfHoldingIdIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();
    List<String> locations = List.of(firstPermanentLocationId, secondPermanentLocationId);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(locations.get(0)), StringValue.of(locations.get(1)));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());

    String instanceId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();

    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withInstanceId(instanceId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withInstanceId(instanceId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertNotNull(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(2, errors.size());
    JsonObject firstPartialError = errors.getJsonObject(0);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", firstPartialError.getString("error"));
    JsonObject secondPartialError = errors.getJsonObject(1);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", secondPartialError.getString("error"));
  }


  @Test
  public void shouldNotProcessEventIfInstanceIdIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();
    List<String> locations = List.of(firstPermanentLocationId, secondPermanentLocationId);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(locations.get(0)), StringValue.of(locations.get(1)));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());

    String instanceId = UUID.randomUUID().toString();
    String firstId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();

    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(firstId)
      .withHrid(firstHrid)
      .withPermanentLocationId(permanentLocationId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withHrid(secondHrid)
      .withPermanentLocationId(permanentLocationId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertNotNull(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(2, errors.size());
    JsonObject firstPartialError = errors.getJsonObject(0);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", firstPartialError.getString("error"));
    JsonObject secondPartialError = errors.getJsonObject(1);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", secondPartialError.getString("error"));
  }

  @Test
  public void shouldNotProcessEventIfPermanentLocationIdIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();
    List<String> locations = List.of(firstPermanentLocationId, secondPermanentLocationId);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(locations.get(0)), StringValue.of(locations.get(1)));

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());

    String instanceId = UUID.randomUUID().toString();
    String firstId = UUID.randomUUID().toString();
    String secondId = UUID.randomUUID().toString();
    String firstHrid = UUID.randomUUID().toString();
    String secondHrid = UUID.randomUUID().toString();

    HoldingsRecord firstHoldingsRecord = new HoldingsRecord()
      .withId(firstId)
      .withHrid(firstHrid)
      .withInstanceId(instanceId);

    HoldingsRecord secondHoldingsRecord = new HoldingsRecord()
      .withId(secondId)
      .withHrid(secondHrid)
      .withInstanceId(instanceId);

    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", firstHoldingsRecord));
    holdingsList.add(new JsonObject().put("holdings", secondHoldingsRecord));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapperForMultipleHoldings)
      .withCurrentNode(profileSnapshotWrapperForMultipleHoldings.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);

    Assert.assertNotNull(dataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(2, errors.size());
    JsonObject firstPartialError = errors.getJsonObject(0);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", firstPartialError.getString("error"));
    JsonObject secondPartialError = errors.getJsonObject(1);
    Assert.assertEquals("Can`t update Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!", secondPartialError.getString("error"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfNoHoldingInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());

    String instanceId = String.valueOf(UUID.randomUUID());
    Instance instance = new Instance(instanceId, "9", String.valueOf(UUID.randomUUID()),
      String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put("InvalidField", Json.encode(instance));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(null)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = mock(Reader.class);

    String permanentLocationId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(HOLDINGS.value(), Json.encode(new HoldingsRecord()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withContentType(ACTION_PROFILE));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(updateHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>());
    assertFalse(updateHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(updateHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Update preliminary Item")
      .withAction(ActionProfile.Action.DELETE)
      .withFolioRecord(HOLDINGS);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(updateHoldingEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotHoldings() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Update preliminary Item")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(actionProfile);
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);
    assertFalse(updateHoldingEventHandler.isEligible(dataImportEventPayload));
  }
}
