package org.folio.inventory.resources.ingest

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class IngestRecordConverter {
  def toJson(records) {
    records.collect {
      def convertedIdentifiers = it.identifiers.collect {
        ["type": "${it.type}", "value": "${it.value}"]
      }

      def convertedCreators = it.creators.collect {
        ["name": "${it.name}"]
      }

      new JsonObject()
        .put("title", it.title)
        .put("barcode", it.barcode)
        .put("identifiers", new JsonArray(convertedIdentifiers))
        .put("creators", new JsonArray(convertedCreators))
    }
  }
}
