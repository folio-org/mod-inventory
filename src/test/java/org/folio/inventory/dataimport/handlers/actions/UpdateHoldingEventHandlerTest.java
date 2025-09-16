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
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.HoldingWriterFactory;
import org.folio.inventory.dataimport.HoldingsMapperFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

import static java.lang.String.format;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.handlers.actions.UpdateHoldingEventHandler.CURRENT_RETRY_NUMBER;
import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        new MappingRule().withName("permanentLocationId").withPath("holdings.permanentLocationId").withValue("\"\\\"Main Library\\\"\"").withEnabled("true"))));

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
    MappingManager.clearMapperFactories();
    MappingManager.clearWriterFactories();
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
    JsonObject resultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(resultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, resultedHoldings.getString("instanceId"));
    Assert.assertEquals(permanentLocationId, resultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(firstHrid, resultedHoldings.getString("hrid"));
    Assert.assertEquals(holdingId, resultedHoldings.getString("id"));
  }

  @Test
  public void shouldUpdateMultipleHoldingsOnOLRetryAndRemoveRetryCounterFromPayloadViaSeveralRuns() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    Reader fakeReader = Mockito.mock(Reader.class);

    // 10 holdingsIds
    List<String> holdingsIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      holdingsIds.add(UUID.randomUUID().toString());
    }

    // 10 hrids for Holdings
    List<String> holdingsHrids = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      holdingsHrids.add(UUID.randomUUID().toString());
    }

    // 10 instanceIds for Holdings
    List<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }

    // 10 permanentLocationIds for Holdings
    List<String> permLocationIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      permLocationIds.add(UUID.randomUUID().toString());
    }

    //actual Holdings which will returned as "actual" after optimistic locking errors
    HoldingsRecord actualHoldings = new HoldingsRecord()
      .withId(holdingsIds.get(0))
      .withHrid(holdingsHrids.get(0))
      .withInstanceId(instanceIds.get(0))
      .withPermanentLocationId(permanentLocationId)
      .withVersion(2);

    HoldingsRecord actualHoldings2 = new HoldingsRecord()
      .withId(holdingsIds.get(3))
      .withHrid(holdingsHrids.get(3))
      .withInstanceId(instanceIds.get(3))
      .withPermanentLocationId(permLocationIds.get(3))
      .withVersion(2);

    HoldingsRecord actualHoldings3 = new HoldingsRecord()
      .withId(holdingsIds.get(4))
      .withHrid(holdingsHrids.get(4))
      .withInstanceId(instanceIds.get(4))
      .withPermanentLocationId(permLocationIds.get(4))
      .withVersion(2);

    HoldingsRecord actualHoldings4 = new HoldingsRecord()
      .withId(holdingsIds.get(5))
      .withHrid(holdingsHrids.get(5))
      .withInstanceId(instanceIds.get(5))
      .withPermanentLocationId(permLocationIds.get(5))
      .withVersion(2);

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    //Real Holdings which will have specific behavior: successful, optimistic locking error (ol), failure by another reason.
    HoldingsRecord olHoldingsRecord1 = new HoldingsRecord()
      .withId(holdingsIds.get(0))
      .withInstanceId(instanceIds.get(0))
      .withHrid(holdingsHrids.get(0))
      .withPermanentLocationId(permLocationIds.get(0));
    HoldingsRecord successfulHoldingsRecord2 = new HoldingsRecord()
      .withId(holdingsIds.get(1))
      .withInstanceId(instanceIds.get(1))
      .withHrid(holdingsHrids.get(1))
      .withPermanentLocationId(permLocationIds.get(1));
    HoldingsRecord partialErrorHoldingsRecord3 = new HoldingsRecord()
      .withId(holdingsIds.get(2))
      .withInstanceId(instanceIds.get(2))
      .withHrid(holdingsHrids.get(2))
      .withPermanentLocationId(permLocationIds.get(2));
    HoldingsRecord olHoldingsRecord4 = new HoldingsRecord()
      .withId(holdingsIds.get(3))
      .withInstanceId(instanceIds.get(3))
      .withHrid(holdingsHrids.get(3))
      .withPermanentLocationId(permLocationIds.get(3));
    HoldingsRecord olHoldingsRecord5 = new HoldingsRecord()
      .withId(holdingsIds.get(4))
      .withInstanceId(instanceIds.get(4))
      .withHrid(holdingsHrids.get(4))
      .withPermanentLocationId(permLocationIds.get(4));
    HoldingsRecord olHoldingsRecord6 = new HoldingsRecord()
      .withId(holdingsIds.get(5))
      .withInstanceId(instanceIds.get(5))
      .withHrid(holdingsHrids.get(5))
      .withPermanentLocationId(permLocationIds.get(5));
    HoldingsRecord successfulHoldingsRecord7 = new HoldingsRecord()
      .withId(holdingsIds.get(6))
      .withInstanceId(instanceIds.get(6))
      .withHrid(holdingsHrids.get(6))
      .withPermanentLocationId(permLocationIds.get(6));
    HoldingsRecord successfulHoldingsRecord8 = new HoldingsRecord()
      .withId(holdingsIds.get(7))
      .withInstanceId(instanceIds.get(7))
      .withHrid(holdingsHrids.get(7))
      .withPermanentLocationId(permLocationIds.get(7));
    HoldingsRecord partialErrorHoldingsRecord9 = new HoldingsRecord()
      .withId(holdingsIds.get(8))
      .withInstanceId(instanceIds.get(8))
      .withHrid(holdingsHrids.get(8))
      .withPermanentLocationId(permLocationIds.get(8));
    HoldingsRecord partialErrorHoldingsRecord10 = new HoldingsRecord()
      .withId(holdingsIds.get(9))
      .withInstanceId(instanceIds.get(9))
      .withHrid(holdingsHrids.get(9))
      .withPermanentLocationId(permLocationIds.get(9  ));
    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord1));
    holdingsList.add(new JsonObject().put("holdings", successfulHoldingsRecord2));
    holdingsList.add(new JsonObject().put("holdings", partialErrorHoldingsRecord3));
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord4));
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord5));
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord6));
    holdingsList.add(new JsonObject().put("holdings", successfulHoldingsRecord7));
    holdingsList.add(new JsonObject().put("holdings", successfulHoldingsRecord8));
    holdingsList.add(new JsonObject().put("holdings", partialErrorHoldingsRecord9));
    holdingsList.add(new JsonObject().put("holdings", partialErrorHoldingsRecord10));

    // Provide behavior for the Holdings
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", holdingsIds.get(0)), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", partialErrorHoldingsRecord3.getId()), 404));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord4.getId()), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord5.getId()), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord6.getId()), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", partialErrorHoldingsRecord9.getId()), 404));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", partialErrorHoldingsRecord10.getId()), 404));
        return null;
      })// 10 Holdings processed. Next iteration will be on the second run 'handle()'-method.
    .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord4.getId()), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", holdingsIds.get(4)), 404));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      }).when(holdingsRecordsCollection).update(any(), any(), any());


    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(actualHoldings, actualHoldings2, actualHoldings3, actualHoldings4), 4);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s OR %s OR %s)", holdingsIds.get(0), holdingsIds.get(3), holdingsIds.get(4), holdingsIds.get(5)))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(holdingsRecordsCollection, times(14)).update(any(), any(), any());
    verify(holdingsRecordsCollection, times(1)).findByCql(any(), any(), any(), any());

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    List<HoldingsRecord> resultedHoldingsRecords = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
    Assert.assertEquals(5, resultedHoldingsRecords.size());
    assertEquals(successfulHoldingsRecord2.getId(), String.valueOf(resultedHoldingsRecords.get(0).getId()));
    assertEquals(successfulHoldingsRecord7.getId(), String.valueOf(resultedHoldingsRecords.get(1).getId()));
    assertEquals(successfulHoldingsRecord8.getId(), String.valueOf(resultedHoldingsRecords.get(2).getId()));
    assertEquals(olHoldingsRecord1.getId(), String.valueOf(resultedHoldingsRecords.get(3).getId()));
    assertEquals(olHoldingsRecord6.getId(), String.valueOf(resultedHoldingsRecords.get(4).getId()));

    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ERRORS));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(5, resultedErrorList.size());
    assertEquals(partialErrorHoldingsRecord3.getId(), String.valueOf(resultedErrorList.get(0).getId()));
    assertEquals(partialErrorHoldingsRecord9.getId(), String.valueOf(resultedErrorList.get(1).getId()));
    assertEquals(partialErrorHoldingsRecord10.getId(), String.valueOf(resultedErrorList.get(2).getId()));
    assertEquals(olHoldingsRecord5.getId(), String.valueOf(resultedErrorList.get(3).getId()));
    assertEquals(olHoldingsRecord4.getId(), String.valueOf(resultedErrorList.get(4).getId()));
    assertEquals(resultedErrorList.get(0).getError(), format("Cannot update record %s not found", partialErrorHoldingsRecord3.getId()));
    assertEquals(resultedErrorList.get(1).getError(), format("Cannot update record %s not found", partialErrorHoldingsRecord9.getId()));
    assertEquals(resultedErrorList.get(2).getError(), format("Cannot update record %s not found", partialErrorHoldingsRecord10.getId()));
    assertEquals(resultedErrorList.get(3).getError(), format("Cannot update record %s not found", olHoldingsRecord5.getId()));
    assertEquals(resultedErrorList.get(4).getError(), format("Current retry number %s exceeded or equal given number %s for the Holding update for jobExecutionId '%s' ", 1, 1, actualDataImportEventPayload.getJobExecutionId()));

    //Second run. We need it to verify that CURRENT_RETRY_NUMBER and all lists are cleared.
    JsonArray holdingsListSecondRun = new JsonArray();
    holdingsListSecondRun.add(new JsonObject().put("holdings", olHoldingsRecord1));
    holdingsListSecondRun.add(new JsonObject().put("holdings", successfulHoldingsRecord2));
    holdingsListSecondRun.add(new JsonObject().put("holdings", partialErrorHoldingsRecord3));
    holdingsListSecondRun.add(new JsonObject().put("holdings", olHoldingsRecord4));

    HashMap<String, String> contextSecondRun = new HashMap<>();
    contextSecondRun.put(HOLDINGS.value(), Json.encode(holdingsListSecondRun));
    contextSecondRun.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayloadSecondRun = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(contextSecondRun)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));


    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord1.getId()), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s not found", partialErrorHoldingsRecord3.getId()), 404));
        return null;
      }).doAnswer(invocationOnMock -> {
        Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
        failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", olHoldingsRecord4.getId()), 409));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .doAnswer(invocationOnMock -> {
        HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
        Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
        successHandler.accept(new Success<>(tmpHoldingsRecord));
        return null;
      })
      .when(holdingsRecordsCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords<HoldingsRecord> result = new MultipleRecords<>(List.of(actualHoldings, actualHoldings2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", holdingsIds.get(0), holdingsIds.get(3)))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    CompletableFuture<DataImportEventPayload> futureSecondRun = updateHoldingEventHandler.handle(dataImportEventPayloadSecondRun);
    DataImportEventPayload actualDataImportEventPayloadSecondRun = futureSecondRun.get(5, TimeUnit.MILLISECONDS);
    verify(holdingsRecordsCollection, times(20)).update(any(), any(), any());
    verify(holdingsRecordsCollection, times(2)).findByCql(any(), any(), any(), any());
    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayloadSecondRun.getEventType());
    Assert.assertNotNull(actualDataImportEventPayloadSecondRun.getContext().get(HOLDINGS.value()));
    Assert.assertNull(actualDataImportEventPayloadSecondRun.getContext().get(CURRENT_RETRY_NUMBER));
    List<HoldingsRecord> resultedHoldingsRecordsSecondRun = List.of(Json.decodeValue(actualDataImportEventPayloadSecondRun.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
    Assert.assertEquals(3, resultedHoldingsRecordsSecondRun.size());

    assertEquals(successfulHoldingsRecord2.getId(), String.valueOf(resultedHoldingsRecordsSecondRun.get(0).getId()));
    assertEquals(olHoldingsRecord1.getId(), String.valueOf(resultedHoldingsRecordsSecondRun.get(1).getId()));
    assertEquals(olHoldingsRecord4.getId(), String.valueOf(resultedHoldingsRecordsSecondRun.get(2).getId()));

    Assert.assertNotNull(actualDataImportEventPayloadSecondRun.getContext().get(ERRORS));
    List<PartialError> resultedErrorListSecondRun = List.of(Json.decodeValue(actualDataImportEventPayloadSecondRun.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(1, resultedErrorListSecondRun.size());
    assertEquals(partialErrorHoldingsRecord3.getId(), String.valueOf(resultedErrorListSecondRun.get(0).getId()));
    assertEquals(resultedErrorListSecondRun.get(0).getError(), format("Cannot update record %s not found", partialErrorHoldingsRecord3.getId()));
  }

  @Test
  public void shouldUpdateSingleHoldingEvenIfOLErrorExistsAndRemoveRetryCounterFromPayload() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String holdingId = UUID.randomUUID().toString();

    String hrid = UUID.randomUUID().toString();

    String instanceId = String.valueOf(UUID.randomUUID());

    String permanentLocationId = UUID.randomUUID().toString();


    HoldingsRecord actualHoldings = new HoldingsRecord()
      .withId(holdingId)
      .withHrid(hrid)
      .withInstanceId(instanceId)
      .withPermanentLocationId(permanentLocationId)
      .withVersion(2);

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    HoldingsRecord olHoldingsRecord1 = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(hrid)
      .withPermanentLocationId(permanentLocationId);
    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord1));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", holdingId), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
      HoldingsRecord tmpHoldingsRecord = invocationOnMock.getArgument(0);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(tmpHoldingsRecord));
      return null;
    }).when(holdingsRecordsCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(actualHoldings), 1);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", holdingId))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(holdingsRecordsCollection, times(2)).update(any(), any(), any());
    verify(holdingsRecordsCollection, times(1)).findByCql(any(), any(), any(), any());

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    List<HoldingsRecord> resultedHoldingsRecords = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
    Assert.assertEquals(1, resultedHoldingsRecords.size());
    assertEquals(olHoldingsRecord1.getId(), String.valueOf(resultedHoldingsRecords.get(0).getId()));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(0, resultedErrorList.size());
  }

  @Test
  public void shouldNotUpdateSingleHoldingIfOLErrorExistsAndRetryNumberIsExceeded() throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String holdingId = UUID.randomUUID().toString();

    String hrid = UUID.randomUUID().toString();

    String instanceId = String.valueOf(UUID.randomUUID());

    String permanentLocationId = UUID.randomUUID().toString();


    HoldingsRecord actualHoldings = new HoldingsRecord()
      .withId(holdingId)
      .withHrid(hrid)
      .withInstanceId(instanceId)
      .withPermanentLocationId(permanentLocationId)
      .withVersion(2);

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(permanentLocationId));
    when(storage.getHoldingsRecordCollection(any())).thenReturn(holdingsRecordsCollection);
    when(storage.getItemCollection(any())).thenReturn(itemCollection);

    HoldingsRecord olHoldingsRecord1 = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withHrid(hrid)
      .withPermanentLocationId(permanentLocationId);
    JsonArray holdingsList = new JsonArray();
    holdingsList.add(new JsonObject().put("holdings", olHoldingsRecord1));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", holdingId), 409));
      return null;
    }).doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(format("Cannot update record %s it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", holdingId), 409));
      return null;
    }).when(holdingsRecordsCollection).update(any(), any(), any());

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(actualHoldings), 1);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", holdingId))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new HoldingWriterFactory());
    MappingManager.registerMapperFactory(new HoldingsMapperFactory());


    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_INSTANCE_ID));
    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), Json.encode(holdingsList));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDING_UPDATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(context)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = updateHoldingEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(5, TimeUnit.MILLISECONDS);
    verify(holdingsRecordsCollection, times(2)).update(any(), any(), any());
    verify(holdingsRecordsCollection, times(1)).findByCql(any(), any(), any(), any());

    Assert.assertEquals(DI_INVENTORY_HOLDING_UPDATED.value(), actualDataImportEventPayload.getEventType());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertNull(actualDataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    List<HoldingsRecord> resultedHoldingsRecords = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord[].class));
    Assert.assertEquals(0, resultedHoldingsRecords.size());
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ERRORS));
    List<PartialError> resultedErrorList = List.of(Json.decodeValue(actualDataImportEventPayload.getContext().get(ERRORS), PartialError[].class));
    Assert.assertEquals(1, resultedErrorList.size());
    assertEquals(holdingId, String.valueOf(resultedErrorList.get(0).getId()));
    assertEquals(format("Current retry number 1 exceeded or equal given number 1 for the Holding update for jobExecutionId '%s' ", actualDataImportEventPayload.getJobExecutionId()), resultedErrorList.get(0).getError());
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
    Assert.assertNotNull(actualDataImportEventPayload.getContext().get(ITEM.value()));
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    JsonObject resultedHoldings = resultedHoldingsList.getJsonObject(0);
    JsonArray resultedItemsList = new JsonArray(actualDataImportEventPayload.getContext().get(ITEM.value()));
    JsonObject resultedItem = resultedItemsList.getJsonObject(0);
    Assert.assertEquals(existingItemJson.getString("id"), resultedItem.getString("id"));
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
    Assert.assertEquals(2, resultedHoldingsList.size());
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
  public void shouldProcessEventAndUpdateMultipleHoldingsWithPartialErrors() throws InterruptedException, ExecutionException, TimeoutException {
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

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordsCollection).update(argThat(holdings -> holdings.getId().equals(firstHoldingsRecord.getId()))
      , any(Consumer.class), any(Consumer.class));

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
    JsonArray errors = new JsonArray(actualDataImportEventPayload.getContext().get(ERRORS));
    Assert.assertEquals(1, resultedHoldingsList.size());
    Assert.assertEquals(1, errors.size());
    JsonObject partialError = errors.getJsonObject(0);
    Assert.assertEquals("Internal Server Error", partialError.getString("error"));
    JsonObject secondResultedHoldings = resultedHoldingsList.getJsonObject(0);
    Assert.assertNotNull(secondResultedHoldings.getString("id"));
    Assert.assertEquals(instanceId, secondResultedHoldings.getString("instanceId"));
    Assert.assertEquals(secondPermanentLocationId, secondResultedHoldings.getString("permanentLocationId"));
    Assert.assertEquals(secondHrid, secondResultedHoldings.getString("hrid"));
    Assert.assertEquals(secondId, secondResultedHoldings.getString("id"));
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnDiErrorWhenNoHoldingsUpdated() throws InterruptedException, ExecutionException, TimeoutException {
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

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordsCollection).update(any(), any(Consumer.class), any(Consumer.class));

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
    future.get(5, TimeUnit.MILLISECONDS);
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
    context.put(ITEM.value(), Json.encode(JsonArray.of(existingItemJson)));
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
    JsonArray resultedHoldingsList = new JsonArray(actualDataImportEventPayload.getContext().get(HOLDINGS.value()));
    Assert.assertEquals(2, resultedHoldingsList.size());
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
    Instance instance = new Instance(instanceId, 9, String.valueOf(UUID.randomUUID()),
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
