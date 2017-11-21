package org.folio.inventory.domain.ingest;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.messaging.JsonMessage;
import org.folio.inventory.domain.Messages;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IngestMessages {
  public static JsonMessage start(
    List<JsonObject> records,
    Map materialTypes,
    Map loanTypes,
    Map locations,
    Map identifierTypes,
    Map instanceTypes,
    Map creatorTypes,
    String jobId,
    Context context) {

    return new JsonMessage(Messages.START_INGEST.Address, headers(jobId, context),
      new JsonObject()
        .put("records", records)
        .put("materialTypes", materialTypes)
        .put("loanTypes", loanTypes)
        .put("locations", locations)
        .put("identifierTypes", identifierTypes)
        .put("instanceTypes", instanceTypes)
        .put("creatorTypes", creatorTypes));
  }

  public static JsonMessage completed(String jobId, Context context) {
    return new JsonMessage(Messages.INGEST_COMPLETED.Address,
      headers(jobId, context), new JsonObject());
  }

  private static Map<String, String> headers(String jobId, Context context) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>(4);
    map.put("jobId", jobId);
    map.put("tenantId", context.getTenantId());
    map.put("token", context.getToken());
    map.put("okapiLocation", context.getOkapiLocation());
    return map;
  }
}
