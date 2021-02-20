package org.folio.inventory.resources;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.HashMap;
import java.util.Map;

public class TenantApi {

  private static final Logger LOG = LogManager.getLogger(TenantApi.class);
  private static final String TENANT_API_PATH = "/_/tenant";
  private static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";

  public void register(Router router) {
    router.post(TENANT_API_PATH).handler(this::postTenant);
  }

  private void postTenant(RoutingContext routingContext) {
    MultiMap headersMap = routingContext.request().headers();
    String tenantId = TenantTool.calculateTenantId(headersMap.get(OKAPI_TENANT_HEADER));
    LOG.info("sending postTenant for " + tenantId);

    Map<String, String> okapiHeaders = getOkapiHeaders(routingContext);
    registerModuleToPubsub(okapiHeaders, routingContext.vertx())
      .onComplete(ar ->
        routingContext.response()
        .setStatusCode(HttpResponseStatus.NO_CONTENT.code())
        .end());
  }

  private Map<String, String> getOkapiHeaders(RoutingContext rc) {
    Map<String, String> okapiHeaders = new HashMap<>();
    rc.request().headers().forEach(headerEntry -> {
      String headerKey = headerEntry.getKey().toLowerCase();
      if (headerKey.startsWith("x-okapi")) {
        okapiHeaders.put(headerKey, headerEntry.getValue());
      }
    });
    return okapiHeaders;
  }

  private Future<Void> registerModuleToPubsub(Map<String, String> headers, Vertx vertx) {
    Promise<Void> promise = Promise.promise();
    PubSubClientUtils.registerModule(new OkapiConnectionParams(headers, vertx))
      .whenComplete((registrationAr, throwable) -> {
        if (throwable == null) {
          LOG.info("Module was successfully registered as publisher/subscriber in mod-pubsub");
          promise.complete();
        } else {
          LOG.error("Error during module registration in mod-pubsub", throwable);
          promise.fail(throwable);
        }
      });
    return promise.future();
  }

}
