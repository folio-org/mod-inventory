package org.folio.inventory.domain.ingest;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.messaging.JsonMessage;
import org.folio.inventory.domain.Messages;
import org.folio.inventory.common.MessagingContext;

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
    Map contributorNameTypes,
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
        .put("contributorNameTypes", contributorNameTypes));
  }

  public static JsonMessage completed(String jobId, Context context) {
    return new JsonMessage(Messages.INGEST_COMPLETED.Address,
      headers(jobId, context), new JsonObject());
  }

  private static Map<String, String> headers(String jobId, Context context) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>(4);
    map.put(MessagingContext.JOB_ID, jobId);
    map.put(MessagingContext.TENANT_ID, context.getTenantId());
    map.put(MessagingContext.TOKEN, context.getToken());
    map.put(MessagingContext.OKAPI_LOCATION, context.getOkapiLocation());
    return map;
  }
}
