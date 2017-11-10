package org.folio.inventory.domain.ingest

import io.vertx.core.json.JsonObject
import org.folio.inventory.common.Context
import org.folio.inventory.common.messaging.JsonMessage
import org.folio.inventory.domain.Messages

class IngestMessages {
  static JsonMessage start(
    records,
    Map materialTypes,
    Map loanTypes,
    Map locations,
    Map identifierTypes,
    Map instanceTypes,
    jobId,
    Context context) {

    new JsonMessage(Messages.START_INGEST.Address,
    headers(jobId, context),
    new JsonObject()
      .put("records", records)
      .put("materialTypes", materialTypes)
      .put("loanTypes", loanTypes)
      .put("locations", locations)
      .put("identifierTypes", identifierTypes)
      .put("instanceTypes", instanceTypes))
  }

  static completed(jobId, Context context) {
    new JsonMessage(Messages.INGEST_COMPLETED.Address,
      headers(jobId, context),
      new JsonObject())
  }

  private static Map<String, String> headers(jobId, Context context) {
    ["jobId"        : jobId,
     "tenantId"     : context.tenantId,
     "token"     : context.token,
     "okapiLocation": context.okapiLocation]
  }
}
