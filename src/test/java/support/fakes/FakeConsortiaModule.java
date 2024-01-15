package support.fakes;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.EndpointFailureHandler;
import org.folio.inventory.support.http.server.JsonResponse;
import support.fakes.processors.RecordPreProcessor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class FakeConsortiaModule extends FakeStorageModule {
  private FakeStorageModule fakeInstanceStorageModule;

  public FakeConsortiaModule(String rootPath, String collectionPropertyName, String tenantId, Collection<String> requiredProperties,
                             boolean hasCollectionDelete, String recordTypeName, Collection<String> uniqueProperties,
                             Map<String, Supplier<Object>> defaultProperties, List<RecordPreProcessor> recordPreProcessors,
                             FakeStorageModule fakeInstanceStorageModule) {
    super(rootPath, collectionPropertyName, tenantId, requiredProperties, hasCollectionDelete, recordTypeName, uniqueProperties, defaultProperties, recordPreProcessors);
    this.fakeInstanceStorageModule = fakeInstanceStorageModule;
  }

  @Override
  void register(Router router) {
    String pathTree = rootPath + "*";
    router.post(pathTree).handler(BodyHandler.create());
    router.put(pathTree).handler(BodyHandler.create());

    router.post(rootPath + "/:id/sharing/instances").handler(this::create);
    router.get(rootPath).handler(this::getMany);
    router.delete(rootPath).handler(this::empty);
  }

  @Override
  protected void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject body = getJsonFromBody(routingContext);

    setDefaultProperties(body);

    String id = body.getString("id");

    createFakeInstance(body)
      .thenCompose(v ->
        createElement(context.getTenantId(), body)
          .thenAccept(notUsed -> {
            System.out.printf("Created %s resource: %s%n", recordTypeName, id);
            JsonResponse.created(routingContext.response(), body);
          })
          .exceptionally(error -> {
            EndpointFailureHandler.handleFailure(EndpointFailureHandler.getKnownException(error),
              routingContext);
            return null;
          }));
  }

  private CompletableFuture<Void> createFakeInstance(JsonObject body) {
    String instanceId = body.getString("instanceIdentifier");
    String targetTenant = body.getString("targetTenantId");
    JsonObject mockInstance = new JsonObject().put("id", instanceId);
    return fakeInstanceStorageModule.createElement(targetTenant, mockInstance);
  }
}
