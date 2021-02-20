package org.folio.inventory.dataimport.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.UUID;

public final class EventHandlingUtil {

  private EventHandlingUtil() {
  }

  private static final Logger LOGGER = LogManager.getLogger(EventHandlingUtil.class);

  /**
   * Prepares and sends event with zipped payload to the mod-pubsub
   *
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param params       connection parameters
   * @return completed future with true if event was sent successfully
   */
  public static Future<Boolean> sendEventWithPayload(String eventPayload, String eventType, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      PubSubClientUtils.sendEventMessage(event, params)
        .whenComplete((ar, throwable) -> {
          if (throwable == null) {
            promise.complete(true);
          } else {
            LOGGER.error("Error during event sending: {}", event, throwable);
            promise.fail(throwable);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to send {} event to mod-pubsub", eventType, e);
      promise.fail(e);
    }
    return promise.future();
  }
}
