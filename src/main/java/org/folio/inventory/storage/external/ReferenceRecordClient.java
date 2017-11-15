package org.folio.inventory.storage.external;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ReferenceRecordClient {
  private final CollectionResourceClient collectionResourceClient;
  private final String collectionWrappingProperty;

  public ReferenceRecordClient(
    CollectionResourceClient collectionResourceClient,
    String collectionWrappingProperty) {

    this.collectionResourceClient = collectionResourceClient;
    this.collectionWrappingProperty = collectionWrappingProperty;
  }

  public CompletableFuture<ReferenceRecord> getRecord(String name)
    throws UnsupportedEncodingException {

    String query = getReferenceRecordQuery(name);

    CompletableFuture<Response> requestFuture = new CompletableFuture<>();

    CompletableFuture<ReferenceRecord> overallFuture
      = new CompletableFuture<>();

    collectionResourceClient.getMany(query, requestFuture::complete);

    requestFuture.thenAccept(response -> {
      if(response == null) {
        overallFuture.completeExceptionally(
          new ReferenceRecordClientException(String.format(
            "Failed to get reference record: %s", name)));
      }
      else if (response.getStatusCode() == 200) {
        List<JsonObject> records = JsonArrayHelper.toList(
          response.getJson().getJsonArray(collectionWrappingProperty));

        if(!records.isEmpty() && records.stream().findFirst().isPresent()) {
          JsonObject referenceRecord = records.stream().findFirst().get();

          overallFuture.complete(new ReferenceRecord(
            referenceRecord.getString("id"),
            referenceRecord.getString("name")
          ));
        }
        else {
          overallFuture.completeExceptionally(
            new ReferenceRecordClientException(
              String.format("Failed to get reference record: %s", name)));
        }
      }
      else {
        overallFuture.completeExceptionally(
          new ReferenceRecordClientException(String.format(
            "Failed to get reference records: %s: %s",
            response.getStatusCode(), response.getBody())));
      }
    });

    return overallFuture;
  }

  private static String getReferenceRecordQuery(String name)
    throws UnsupportedEncodingException {

    return "query=" + URLEncoder.encode(String.format("name=\"%s\"", name), "UTF-8");
  }

  public class ReferenceRecordClientException extends Exception {
    private ReferenceRecordClientException(String message) {
      super(message);
    }
  }
}
