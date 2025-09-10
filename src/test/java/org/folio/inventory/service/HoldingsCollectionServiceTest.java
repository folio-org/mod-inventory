package org.folio.inventory.service;

import io.vertx.core.Future;
import org.folio.HoldingsRecordsSource;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.services.HoldingsCollectionService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

public class HoldingsCollectionServiceTest {
  private HoldingsCollectionService holdingsCollectionService;

  @Mock
  HoldingsRecordsSourceCollection holdingsRecordsSourceCollection;
  @Mock
  InstanceCollection instanceRecordCollection;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    holdingsCollectionService = new HoldingsCollectionService();
  }

  @Test
  public void shouldFindSourceId() throws IOException {
    var sourceId = String.valueOf(UUID.randomUUID());
    doAnswer(invocationOnMock -> {
      HoldingsRecordsSource source = new HoldingsRecordsSource().withId(sourceId).withName("MARC");
      List<HoldingsRecordsSource> sourceList = Collections.singletonList(source);
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(sourceList, 1);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findSourceIdByName(holdingsRecordsSourceCollection, "MARC");
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
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertEquals(instanceId, future.result());
  }


  @Test
  public void shouldFailWhenExceptionByFindInstanceId() throws IOException {
    doThrow(new UnsupportedEncodingException())
      .when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenExceptionByFindSourceId() throws IOException {
    doThrow(new UnsupportedEncodingException())
      .when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findSourceIdByName(holdingsRecordsSourceCollection, "MARC");
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenFindSourceIdFailure() throws IOException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findSourceIdByName(holdingsRecordsSourceCollection, "MARC");
    assertTrue(future.failed());
  }

  @Test()
  public void shouldFailWhenFindInstanceIdFailure() throws IOException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    Future<String> future = holdingsCollectionService.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
  }

  @Test
  public void shouldFailIfSourceIdNotFound() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    var message = "No source id found for holdings with name MARC";
    Future<String> future = holdingsCollectionService.findSourceIdByName(holdingsRecordsSourceCollection, "MARC");
    assertTrue(future.failed());
    assertEquals(future.cause().getMessage(), message);
  }

  @Test
  public void shouldFailIfInstanceIdNotFound() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 0);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    var message = "No instance id found for marc holdings with hrid: in00000000315";
    Future<String> future = holdingsCollectionService.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
    assertTrue(future.failed());
    assertEquals(future.cause().getMessage(), message);
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfFindWrongRecordByHrid() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<Instance> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<Instance>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(instanceRecordCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    holdingsCollectionService.findInstanceIdByHrid(instanceRecordCollection, "in00000000315");
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfFindWrongRecordByName() throws IOException {
    doAnswer(invocationOnMock -> {
      MultipleRecords<HoldingsRecordsSource> result = new MultipleRecords<>(new ArrayList<>(), 1);
      Consumer<Success<MultipleRecords<HoldingsRecordsSource>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(holdingsRecordsSourceCollection).findByCql(anyString(), any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    holdingsCollectionService.findSourceIdByName(holdingsRecordsSourceCollection, "MARC");
  }

}
