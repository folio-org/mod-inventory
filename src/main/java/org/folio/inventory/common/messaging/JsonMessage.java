package org.folio.inventory.common.messaging

import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.core.json.JsonObject

class JsonMessage {
  private final String address
  private final Map headers
  private final JsonObject body

  JsonMessage(String address, Map headers, JsonObject body) {
    this.address = address
    this.headers = headers
    this.body = body
  }

  void send(Vertx vertx) {
    send(vertx.eventBus())
  }

  void send(EventBus eventBus) {
    def options = new DeliveryOptions()

    headers.each { options.addHeader(it.key, it.value) }

    eventBus.send(
      address,
      body,
      options)
  }
}
