package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.rest.jaxrs.model.Event;

import static org.folio.DataImportEventTypes.DI_ERROR;

/**
 * Error handler for Data Import kafka consumers that sends {@code DI_ERROR} events
 * when the business handler fails. This ensures that mod-source-record-manager
 * properly tracks progress and the import finishes with status 'Completed with errors'
 * instead of hanging.
 */
public class DataImportProcessRecordErrorHandler implements ProcessRecordErrorHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportProcessRecordErrorHandler.class);
  private static final String ERROR_KEY = "ERROR";

  private final KafkaConfig kafkaConfig;
  private final Vertx vertx;

  public DataImportProcessRecordErrorHandler(KafkaConfig kafkaConfig, Vertx vertx) {
    this.kafkaConfig = kafkaConfig;
    this.vertx = vertx;
  }

  @Override
  public void handle(Throwable cause, KafkaConsumerRecord<String, String> record) {
    if (cause instanceof DuplicateEventException) {
      LOGGER.info("handle:: Skipping DI_ERROR for duplicate event, record key: '{}'", record.key());
      return;
    }

    try {
      Event event = Json.decodeValue(record.value(), Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);

      eventPayload.getEventsChain().add(eventPayload.getEventType());
      eventPayload.getContext().put(ERROR_KEY, cause.getMessage());
      eventPayload.setEventType(DI_ERROR.value());

      try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
        eventPublisher.publish(eventPayload);
        LOGGER.warn("handle:: Sent DI_ERROR event for jobExecutionId: '{}', recordId: '{}', error: '{}'",
          eventPayload.getJobExecutionId(),
          eventPayload.getContext().get("recordId"),
          cause.getMessage());
      }
    } catch (Exception e) {
      LOGGER.error("handle:: Failed to send DI_ERROR event for record key: '{}'", record.key(), e);
    }
  }
}

