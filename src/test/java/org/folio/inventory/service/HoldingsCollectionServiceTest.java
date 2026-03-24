package org.folio.inventory.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import io.vertx.core.Future;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.HoldingsRecord;
import org.folio.HoldingsRecordsSource;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.services.HoldingsCollectionService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HoldingsCollectionServiceTest {

  private static final String MARC_SOURCE = "MARC";
  private final HoldingsCollectionService service = new HoldingsCollectionService();

  @Mock
  private HoldingsRecordsSourceCollection holdingsRecordsSourceCollection;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollection;

  @Test
  public void shouldFindSourceId() throws IOException {
    var sourceId = String.valueOf(UUID.randomUUID());
    doAnswer(invocationOnMock -> {
      HoldingsRecordsSource source = new HoldingsRecordsSource().withId(sourceId).withName(MARC_SOURCE);
      List<HoldingsRecordsSource> sourceList = Collections.singletonList(source);
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(sourceList, 1);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findSourceIdByName(holdingsRecordsSourceCollection, MARC_SOURCE);
    assertEquals(sourceId, future.result());
  }

  @Test
  public void shouldFindInstanceId() throws IOException {
    var instanceId = String.valueOf(UUID.randomUUID());
    doAnswer(invocationOnMock -> {

      Instance instance = new Instance(instanceId, 2, String.valueOf(UUID.randomUUID()),
        String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()), String.valueOf(UUID.randomUUID()));
      List<Instance> instanceList = Collections.singletonList(instance);
      MultipleRecords<Instance> result = new MultipleRecords<>(instanceList, 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertEquals(instanceId, future.result());
  }

  @Test
  public void shouldFailWhenExceptionByFindInstanceId() throws IOException {
    doThrow(new UnsupportedEncodingException())
      .when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenExceptionByFindSourceId() throws IOException {
    doThrow(new UnsupportedEncodingException())
      .when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findSourceIdByName(holdingsRecordsSourceCollection, MARC_SOURCE);
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenFindSourceIdFailure() throws IOException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findSourceIdByName(holdingsRecordsSourceCollection, MARC_SOURCE);
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenFindInstanceIdFailure() throws IOException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    Future<String> future = service.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
  }

  @Test
  public void shouldFailIfSourceIdNotFound() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    var message = "No source id found for holdings with name MARC";
    Future<String> future = service.findSourceIdByName(holdingsRecordsSourceCollection, MARC_SOURCE);
    assertTrue(future.failed());
    assertEquals(message, future.cause().getMessage());
  }

  @Test
  public void shouldFailIfInstanceIdNotFound() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    var message = "No instance id found for marc holdings with hrid: in00000000315";
    Future<String> future = service.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
    assertEquals(message, future.cause().getMessage());
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfFindWrongRecordByHrid() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    service.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfFindWrongRecordByName() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(), any());

    service.findSourceIdByName(holdingsRecordsSourceCollection, MARC_SOURCE);
  }

  @Test
  public void shouldGetHoldingsRecordById() {
    var holdingsId = UUID.randomUUID().toString();
    var instanceId = UUID.randomUUID().toString();
    doAnswer(invocationOnMock -> {
      HoldingsRecord holdingsRecord = new HoldingsRecord().withId(holdingsId).withInstanceId(instanceId);
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(holdingsRecord));
      return null;
    }).when(holdingsRecordCollection).findById(anyString(), any(), any());

    Future<HoldingsRecord> future = service.getById(holdingsId, holdingsRecordCollection);
    assertTrue(future.succeeded());
    assertEquals(holdingsId, future.result().getId());
    assertEquals(instanceId, future.result().getInstanceId());
  }

  @Test
  public void shouldFailWhenHoldingsRecordNotFoundById() {
    var holdingsId = UUID.randomUUID().toString();
    doAnswer(invocationOnMock -> {
      Consumer<Success<HoldingsRecord>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(holdingsRecordCollection).findById(anyString(), any(), any());

    Future<HoldingsRecord> future = service.getById(holdingsId, holdingsRecordCollection);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof NotFoundException);
    assertEquals("Can't find Holdings by id: " + holdingsId + " ", future.cause().getMessage());
  }

  @Test
  public void shouldFailWhenGetByIdFailure() {
    var holdingsId = UUID.randomUUID().toString();
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordCollection).findById(anyString(), any(), any());

    Future<HoldingsRecord> future = service.getById(holdingsId, holdingsRecordCollection);
    assertTrue(future.failed());
    assertEquals("Internal Server Error", future.cause().getMessage());
  }

  @Test
  public void shouldUpdateHoldingsRecord() {
    var holdingsId = UUID.randomUUID().toString();
    var instanceId = UUID.randomUUID().toString();
    HoldingsRecord holdingsRecord = new HoldingsRecord().withId(holdingsId).withInstanceId(instanceId);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Void>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(holdingsRecordCollection).update(any(HoldingsRecord.class), any(), any());

    Future<HoldingsRecord> future = service.update(holdingsRecord, holdingsRecordCollection);
    assertTrue(future.succeeded());
    assertEquals(holdingsId, future.result().getId());
    assertEquals(instanceId, future.result().getInstanceId());
  }

  @Test
  public void shouldFailWhenUpdateHoldingsRecordWithConflict() {
    var holdingsId = UUID.randomUUID().toString();
    HoldingsRecord holdingsRecord = new HoldingsRecord().withId(holdingsId);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Conflict - version mismatch", 409));
      return null;
    }).when(holdingsRecordCollection).update(any(HoldingsRecord.class), any(), any());

    Future<HoldingsRecord> future = service.update(holdingsRecord, holdingsRecordCollection);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof OptimisticLockingException);
    assertEquals("Conflict - version mismatch", future.cause().getMessage());
  }

  @Test
  public void shouldFailWhenUpdateHoldingsRecordFailure() {
    var holdingsId = UUID.randomUUID().toString();
    HoldingsRecord holdingsRecord = new HoldingsRecord().withId(holdingsId);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordCollection).update(any(HoldingsRecord.class), any(), any());

    Future<HoldingsRecord> future = service.update(holdingsRecord, holdingsRecordCollection);
    assertTrue(future.failed());
    assertEquals("Internal Server Error", future.cause().getMessage());
  }
}
