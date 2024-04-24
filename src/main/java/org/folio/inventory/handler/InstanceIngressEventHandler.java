package org.folio.inventory.handler;

import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.AsyncRecordHandler;

public class InstanceIngressEventHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(
    InstanceIngressEventHandler.class);

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> kafkaConsumerRecord) {
    // to extract and re-use common logic from CreateInstanceEventHandler
    // 1. Change event; 2. Re-use all except: source type to be changed to BIBFRAME, DI event not to be sent

    return null;
  }

}
