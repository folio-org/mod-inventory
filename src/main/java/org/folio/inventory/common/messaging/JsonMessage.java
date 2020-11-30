package org.folio.inventory.common.messaging;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import java.util.Map;

public class JsonMessage {
  public JsonMessage(String address, Map headers, JsonObject body) {
    this.address = address;
    this.headers = headers;
    this.body = body;
  }

  public void send(Vertx vertx) {
    send(vertx.eventBus());
  }

  public void send(EventBus eventBus) {
    final DeliveryOptions options = new DeliveryOptions();

    headers.forEach((key, value) -> options.addHeader(key.toString(), value.toString()));

    eventBus.send(address, body, options);
  }

  private final String address;
  private final Map headers;
  private final JsonObject body;
}
