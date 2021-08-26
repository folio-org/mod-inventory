package org.folio.inventory.dataimport.consumers;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.inventory.domain.Holding;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class MarcBibHoldingsHridSetKafkaHandlerTest {

  private static final String RECORD_PATH = "";
  private static final String HOLDING_PATH = "";

  @Mock
  private Storage mockedStorage;
  @Mock
  private HoldingsRecordCollection holdingsRecordCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private KafkaInternalCache kafkaInternalCache;


  private Record record;
  private Holding existingHolding;
  private MarcHoldingsHridSetKafkaHandler marcHoldingsHridSetKafkaHandler;

  @Before
  public void setUp() throws IOException {
  }

  @Test
  public void shouldReturnSucceededFuture(TestContext context) throws IOException {

  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadHasNoHoldingsRecord(TestContext context) throws IOException {

  }

  @Test
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {

  }

  @Test
  public void shouldNotHandleIfCacheAlreadyContainsThisEvent(TestContext context) throws IOException {

  }


}
