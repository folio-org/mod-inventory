package org.folio.inventory.dataimport.services;

import static java.lang.String.format;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.inventory.client.wrappers.SourceStorageSnapshotsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotService {

  private static final Logger LOGGER = LogManager.getLogger(SnapshotService.class);
  private final HttpClient httpClient;

  public SnapshotService(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public Future<Snapshot> postSnapshotInSrsAndHandleResponse(Context context, Snapshot snapshot) {
    Promise<Snapshot> promise = Promise.promise();
    getSourceStorageSnapshotsClient(context).postSourceStorageSnapshots(snapshot)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.info("postSnapshotInSrsAndHandleResponse:: Posted snapshot with id: {} to tenant: {}",
            snapshot.getJobExecutionId(), context.getTenantId());
          promise.complete(result.bodyAsJson(Snapshot.class));
        } else {
          String msg =
            format("Failed to create snapshot in SRS, snapshot id: %s, tenant id: %s, status code: %s, snapshot: %s",
              snapshot.getJobExecutionId(), context.getTenantId(), result != null ? result.statusCode() : "",
              result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          promise.fail(msg);
        }
      });
    return promise.future();
  }

  public SourceStorageSnapshotsClient getSourceStorageSnapshotsClient(Context context) {
    var folioHeaders = FolioHeaders.builder()
      .connectionUrl(context.getOkapiLocation())
      .userId(context.getUserId())
      .token(context.getToken())
      .requestId(context.getRequestId())
      .tenant(context.getTenantId());
    return new SourceStorageSnapshotsClientWrapper(folioHeaders, httpClient);
  }
}
