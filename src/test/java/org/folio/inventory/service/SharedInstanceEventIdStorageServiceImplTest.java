package org.folio.inventory.service;

import io.vertx.core.Future;
import org.folio.inventory.common.dao.EventIdStorageDaoImpl;
import org.folio.inventory.domain.relationship.EventTable;
import org.folio.inventory.domain.relationship.EventToEntity;
import org.folio.inventory.services.SharedInstanceEventIdStorageServiceImpl;
import org.folio.kafka.exception.DuplicateEventException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SharedInstanceEventIdStorageServiceImplTest {
  private static final String EVENT_ID = UUID.randomUUID().toString();
  private static final String INSTANCE_ID = UUID.randomUUID().toString();

  @Mock
  private EventIdStorageDaoImpl eventIdStorageDaoImpl;
  @InjectMocks
  private SharedInstanceEventIdStorageServiceImpl sharedInstanceEventIdStorageService;

  @Test
  public void shouldReturnSavedEventId() {
    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(EVENT_ID).build();
    when(eventIdStorageDaoImpl.storeEvent(any(EventToEntity.class), any())).thenReturn(Future.succeededFuture(EVENT_ID));
    Future<String> future = sharedInstanceEventIdStorageService.store(EVENT_ID, TENANT_ID);

    String savedEventId = future.result();
    assertEquals(eventToEntity.getEventId(), savedEventId);
  }

  @Test
  public void shouldReturnFailedFuture() {
    when(eventIdStorageDaoImpl.storeEvent(any(EventToEntity.class), any())).thenReturn(Future.failedFuture(new DuplicateEventException("Testing Error Message")));
    Future<String> future = sharedInstanceEventIdStorageService.store(EVENT_ID, TENANT_ID);

    assertEquals("Testing Error Message", future.cause().getMessage());
  }
}
