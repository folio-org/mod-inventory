package org.folio.inventory.dataimport.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.client.wrappers.SourceStorageSnapshotsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.Snapshot;

import static java.lang.String.format;

public class SnapshotService {

  @Getter
  private final HttpClient httpClient;

  private static final Logger LOGGER = LogManager.getLogger(SnapshotService.class);

  public SnapshotService(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public SourceStorageSnapshotsClient getSourceStorageSnapshotsClient(String okapiUrl, String token, String tenantId, String userId, String requestId) {
    return new SourceStorageSnapshotsClientWrapper(okapiUrl, tenantId, token, userId, requestId, getHttpClient());
  }

  public Future<Snapshot> postSnapshotInSrsAndHandleResponse(Context context, Snapshot snapshot) {
    Promise<Snapshot> promise = Promise.promise();
    getSourceStorageSnapshotsClient(context.getOkapiLocation(), context.getToken(),
      context.getTenantId(), context.getUserId(), context.getRequestId()).postSourceStorageSnapshots(snapshot)
      .onComplete(ar -> {
        var result = ar.result();
        if (ar.succeeded() && result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.info("postSnapshotInSrsAndHandleResponse:: Posted snapshot with id: {} to tenant: {}", snapshot.getJobExecutionId(), context.getTenantId());
          promise.complete(result.bodyAsJson(Snapshot.class));
        } else {
          String msg = format("Failed to create snapshot in SRS, snapshot id: %s, tenant id: %s, status code: %s, snapshot: %s",
            snapshot.getJobExecutionId(), context.getTenantId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
          LOGGER.warn(msg);
          promise.fail(msg);
        }
      });
    return promise.future();
  }
}
