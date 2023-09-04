package org.folio.inventory.consortium;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;

public class ConsortiumInstanceSharingErrorHandler implements ProcessRecordErrorHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, String> kafkaConsumerRecord) {
    LOGGER.info("kafkaConsumerRecord.key : {}", kafkaConsumerRecord.key());
    LOGGER.info("kafkaConsumerRecord.value : {}", kafkaConsumerRecord.value());
    LOGGER.info("kafkaConsumerRecord.headers : {} ",  KafkaHeaderUtils.kafkaHeadersToMap(kafkaConsumerRecord.headers()));
    LOGGER.info("kafkaConsumerRecord.headers.size : {} ",  KafkaHeaderUtils.kafkaHeadersToMap(kafkaConsumerRecord.headers()).size());
  }

}
