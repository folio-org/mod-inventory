package support.fakes;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.EndpointFailureHandler;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.joda.time.DateTime;
import support.fakes.processors.RecordPreProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE;
import static java.util.concurrent.CompletableFuture.completedFuture;

class FakeStorageModule extends AbstractVerticle {
  private final String rootPath;
  private final String collectionPropertyName;
  private final boolean hasCollectionDelete;
  private final Collection<String> requiredProperties;
  private final Map<String, Map<String, JsonObject>> storedResourcesByTenant;
  private final String recordTypeName;
  private final Collection<String> uniqueProperties;
  private final Map<String, Supplier<Object>> defaultProperties;
  private final List<RecordPreProcessor> recordPreProcessors;
  private EndpointFailureDescriptor endpointFailureDescriptor = null;

  FakeStorageModule(
    String rootPath,
    String collectionPropertyName,
    List<String> tenants,
    Collection<String> requiredProperties,
    boolean hasCollectionDelete,
    String recordTypeName,
    Collection<String> uniqueProperties,
    Map<String, Supplier<Object>> defaultProperties,
    List<RecordPreProcessor> recordPreProcessors) {

    this.rootPath = rootPath;
    this.collectionPropertyName = collectionPropertyName;
    this.requiredProperties = requiredProperties;
    this.hasCollectionDelete = hasCollectionDelete;
    this.recordTypeName = recordTypeName;
    this.uniqueProperties = uniqueProperties;

    HashMap<String, Supplier<Object>> defaultPropertiesWithId = new HashMap<>(defaultProperties);

    defaultPropertiesWithId.put("id", () -> UUID.randomUUID().toString());

    this.defaultProperties = defaultPropertiesWithId;

    storedResourcesByTenant = new HashMap<>();
    tenants.forEach(tenant -> storedResourcesByTenant.put(tenant, new HashMap<>()));
    this.recordPreProcessors = recordPreProcessors;
  }

  void register(Router router) {
    String pathTree = rootPath + "*";

    router.post(pathTree).handler(BodyHandler.create());
    router.put(pathTree).handler(BodyHandler.create());

    router.route(pathTree).handler(this::emulateFailureIfNeeded);
    router.route(pathTree).handler(this::checkTokenHeader);

    router.put(rootPath + "/:id/suppress-from-discovery").handler(this::successSuppressFromDiscovery);

    router.post(rootPath + "/retrieve").handler(this::retrieveMany);
    router.post(rootPath).handler(this::checkRequiredProperties);
    router.post(rootPath).handler(this::checkUniqueProperties);
    router.post(rootPath + "/emulate-failure").handler(this::emulateFailure);
    router.post(rootPath).handler(this::create);

    router.get(rootPath).handler(this::getMany);
    router.delete(rootPath).handler(this::empty);

    router.put(rootPath + "/:id").handler(this::checkRequiredProperties);
    router.put(rootPath + "/:id").handler(this::replace);

    router.get(rootPath + "/:id").handler(this::get);
    router.delete(rootPath + "/:id").handler(this::delete);

    router.get(rootPath + "/:id/formatted").handler(this::getByExternalId);
    router.post("/source-storage/snapshots").handler(this::createSnapshot);
  }

  private void emulateFailureIfNeeded(RoutingContext routingContext) {
    if (shouldEmulateFailure(routingContext)) {
      final String body = endpointFailureDescriptor.getBody();
      String bodyContains = endpointFailureDescriptor.getBodyContains();

      if (bodyContains != null) {
        String requestBody = routingContext.body().asString();
        if (requestBody != null && requestBody.contains(bodyContains)) {
          routingContext.response()
            .setStatusCode(endpointFailureDescriptor.getStatusCode())
            .putHeader(HttpHeaders.CONTENT_TYPE, endpointFailureDescriptor.getContentType())
            .end(body);
        } else {
          routingContext.next();
        }
        return;
      }

      routingContext.response()
        .setStatusCode(endpointFailureDescriptor.getStatusCode())
        .putHeader(HttpHeaders.CONTENT_TYPE, endpointFailureDescriptor.getContentType())
        .end(body);

    } else {
      routingContext.next();
    }
  }

  private boolean shouldEmulateFailure(RoutingContext routingContext) {
    if (routingContext.request().uri().endsWith("/emulate-failure")) {
      return false;
    }

    return endpointFailureDescriptor != null && DateTime.now().toDate()
      .before(endpointFailureDescriptor.getFailureExpireDate())
      && endpointFailureDescriptor.getMethod().equals(routingContext.request()
      .method().name());
  }

  void registerBatch(Router router, String batchPath) {
    String pathTree = batchPath + "/*";

    router.post(pathTree).handler(BodyHandler.create());
    router.post(batchPath).handler(BodyHandler.create());
    router.route(batchPath).handler(this::checkTokenHeader);
    router.post(batchPath).handler(this::createBatch);
  }

  private void createBatch(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    JsonObject body = getJsonFromBody(routingContext);
    JsonArray batchElements = body.getJsonArray(collectionPropertyName);

    CompletableFuture<Void> lastCreate = completedFuture(null);

    for (int i = 0; i < batchElements.size(); i++) {
      JsonObject element = batchElements.getJsonObject(i);
      setDefaultProperties(element);
      String id = element.getString("id");

      lastCreate = lastCreate.thenCompose(prev -> createElement(context, element));

      System.out.printf("Created %s resource: %s%n", recordTypeName, id);
    }

    lastCreate.thenAccept(notUsed -> {
      JsonObject responseBody = new JsonObject()
        .put(collectionPropertyName, batchElements)
        .put("errorMessages", new JsonArray())
        .put("totalRecords", batchElements.size());
      JsonResponse.created(routingContext.response(), responseBody);
    });
  }

  private void create(RoutingContext routingContext) {

    WebContext context = new WebContext(routingContext);

    JsonObject body = getJsonFromBody(routingContext);

    setDefaultProperties(body);

    String id = body.getString("id");

    createElement(context, body).thenAccept(notUsed -> {
      System.out.printf("Created %s resource: %s%n", recordTypeName, id);

      JsonResponse.created(routingContext.response(), body);
    }).exceptionally(error -> {
      EndpointFailureHandler.handleFailure(EndpointFailureHandler.getKnownException(error),
        routingContext);

      return null;
    });
  }

  private CompletableFuture<Void> createElement(WebContext context, JsonObject rawBody) {
    String id = rawBody.getString("id");

    return preProcessRecords(context.getTenantId(), null, rawBody).thenAccept(body -> getResourcesForTenant(context).put(id, body));
  }

  private void replace(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    JsonObject rawBody = getJsonFromBody(routingContext);
    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    preProcessRecords(context.getTenantId(), resourcesForTenant.get(id), rawBody).thenAccept(body -> {
      setDefaultProperties(body);

      if (ID_FOR_FAILURE.toString().equals(id)) {
        ServerErrorResponse.internalError(routingContext.response(), "Test Internal Server Error");
      } else if (ID_FOR_OPTIMISTIC_LOCKING_FAILURE.toString().equals(id)) {
        ClientErrorResponse.optimisticLocking(routingContext.response(), "Optimistic Locking");
      } else if (resourcesForTenant.containsKey(id)) {
        System.out.printf("Replaced %s resource: %s%n", recordTypeName, id);

        resourcesForTenant.replace(id, body);
        SuccessResponse.noContent(routingContext.response());
      } else {
        System.out.printf("Created %s resource: %s%n", recordTypeName, id);

        resourcesForTenant.put(id, body);
        SuccessResponse.noContent(routingContext.response());
      }
    });
  }

  private void get(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if (resourcesForTenant.containsKey(id)) {
      final JsonObject resourceRepresentation = resourcesForTenant.get(id);

      System.out.printf("Found %s resource: %s%n", recordTypeName,
        resourceRepresentation.encodePrettily());

      JsonResponse.success(routingContext.response(), resourceRepresentation);
    } else {
      System.out.printf("Failed to find %s resource: %s%n", recordTypeName, id);

      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void getMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    Integer limit = context.getIntegerParameter("limit", 10);
    Integer offset = context.getIntegerParameter("offset", 0);
    String query = context.getStringParameter("query", null);

    System.out.printf("Handling %s%n", routingContext.request().uri());

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    List<JsonObject> filteredItems = new FakeCQLToJSONInterpreter(false)
      .execute(resourcesForTenant.values(), query);

    List<JsonObject> pagedItems = filteredItems.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());

    JsonObject result = new JsonObject();

    result.put(collectionPropertyName, new JsonArray(pagedItems));
    result.put("totalRecords", filteredItems.size());

    System.out.printf("Found %s resources: %s%n", recordTypeName,
      result.encodePrettily());

    JsonResponse.success(routingContext.response(), result);
  }

  private void retrieveMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    var requestBody = routingContext.getBodyAsJson();

    var limit = requestBody.getInteger("limit");
    var offset = requestBody.getInteger("offset");
    var query = requestBody.getString("query");

    System.out.printf("Handling %s%n", routingContext.request().uri());

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    List<JsonObject> filteredItems = new FakeCQLToJSONInterpreter(false)
      .execute(resourcesForTenant.values(), query);

    List<JsonObject> pagedItems = filteredItems.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());

    JsonObject result = new JsonObject();

    result.put(collectionPropertyName, new JsonArray(pagedItems));
    result.put("totalRecords", filteredItems.size());

    System.out.printf("Found %s resources: %s%n", recordTypeName,
      result.encodePrettily());

    JsonResponse.success(routingContext.response(), result);
  }

  private void empty(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    if (!hasCollectionDelete) {
      ClientErrorResponse.notFound(routingContext.response());
      return;
    }

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);
    List<String> queries = routingContext.queryParam("query");
    String query = queries.size() == 1 ? queries.getFirst() : "";

    if (query.startsWith("id==")) {
      resourcesForTenant.remove(query.substring(4));
    } else {
      resourcesForTenant.clear();
    }

    SuccessResponse.noContent(routingContext.response());
  }

  private void successSuppressFromDiscovery(RoutingContext routingContext) {
    var id = routingContext.request().getParam("id");
    var resourcesForTenant = getResourcesForTenant(new WebContext(routingContext));
    if (resourcesForTenant.containsKey(id)) {
      resourcesForTenant.get(id).getJsonObject("additionalInfo").put("suppressDiscovery", true);
      JsonResponse.success(routingContext.response(), new JsonObject());
    } else {
      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void delete(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if (resourcesForTenant.containsKey(id)) {
      System.out.printf("Deleted %s resource: %s%n", recordTypeName, id);
      resourcesForTenant.remove(id);

      SuccessResponse.noContent(routingContext.response());
    } else {
      System.out.printf("%s resource: %s%n for deletion is not found", recordTypeName, id);
      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private Map<String, JsonObject> getResourcesForTenant(WebContext context) {
    return storedResourcesByTenant.get(context.getTenantId());
  }

  private static JsonObject getJsonFromBody(RoutingContext routingContext) {
    if (hasBody(routingContext)) {
      return routingContext.getBodyAsJson();
    } else {
      return new JsonObject();
    }
  }

  private static boolean hasBody(RoutingContext routingContext) {
    return StringUtils.isNotBlank(routingContext.getBodyAsString());
  }

  private void checkTokenHeader(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    if (StringUtils.isBlank(context.getToken())) {
      ClientErrorResponse.forbidden(routingContext.response());
    } else {
      routingContext.next();
    }
  }

  private void checkRequiredProperties(RoutingContext routingContext) {
    JsonObject body = getJsonFromBody(routingContext);

    List<ValidationError> errors = new ArrayList<>();

    requiredProperties.forEach(requiredProperty -> {
      if (getPropertyValue(body, requiredProperty) == null) {
        errors.add(new ValidationError("Required property missing", requiredProperty, ""));
      }
    });

    if (errors.isEmpty()) {
      routingContext.next();
    } else {
      JsonResponse.unprocessableEntity(routingContext.response(), errors);
    }
  }

  private Object getPropertyValue(JsonObject body, String requiredProperty) {
    String[] pathElements = requiredProperty.split("\\.");
    JsonObject lastObject = body;

    for (int i = 0; i < pathElements.length - 1; i++) {
      lastObject = lastObject.getJsonObject(pathElements[i]);
    }

    return lastObject != null
      ? lastObject.getValue(pathElements[pathElements.length - 1])
      : null;
  }

  private void checkUniqueProperties(RoutingContext routingContext) {
    if (uniqueProperties.isEmpty()) {
      routingContext.next();
      return;
    }

    JsonObject body = getJsonFromBody(routingContext);

    ArrayList<ValidationError> errors = new ArrayList<>();

    uniqueProperties.forEach(uniqueProperty -> {
      String proposedValue = body.getString(uniqueProperty);

      Map<String, JsonObject> records = getResourcesForTenant(new WebContext(routingContext));

      if (records.values().stream()
        .map(record -> record.getString(uniqueProperty))
        .anyMatch(usedValue -> usedValue.equals(proposedValue))) {

        errors.add(new ValidationError(
          String.format("%s with this %s already exists", recordTypeName, uniqueProperty),
          uniqueProperty, proposedValue));

        JsonResponse.unprocessableEntity(routingContext.response(),
          errors);
      }
    });

    if (errors.isEmpty()) {
      routingContext.next();
    }
  }

  private void setDefaultProperties(JsonObject representation) {
    defaultProperties.forEach((property, valueSupplier) -> {
      if (!representation.containsKey(property)) {
        representation.put(property, valueSupplier.get());
      }
    });
  }

  private CompletableFuture<JsonObject> preProcessRecords(String tenant, JsonObject oldBody, JsonObject newBody) {
    CompletableFuture<JsonObject> lastPreProcess = completedFuture(newBody);

    for (RecordPreProcessor preProcessor : recordPreProcessors) {
      lastPreProcess = lastPreProcess
        .thenCompose(prev -> {
            try {
              return preProcessor.process(tenant, oldBody, newBody);
            } catch (Exception ex) {
              CompletableFuture<JsonObject> future = new CompletableFuture<>();
              future.completeExceptionally(ex);

              return future;
            }
          }
        );
    }

    return lastPreProcess;
  }

  private void createSnapshot(RoutingContext context) {
    JsonObject snapshotRequest = context.body().asJsonObject();
    JsonObject snapshotResponse = (snapshotRequest != null) ? snapshotRequest : new JsonObject();
    snapshotResponse.put("status", "COMMITTED");
    if (!snapshotResponse.containsKey("jobExecutionId")) {
      snapshotResponse.put("jobExecutionId", UUID.randomUUID().toString());
    }

    JsonResponse.created(context.response(), snapshotResponse);
  }

  private void getByExternalId(RoutingContext routingContext) {
    final String idType = routingContext.request().getParam("idType");
    if (!"HOLDINGS".equals(idType)) {
      ClientErrorResponse.notFound(routingContext.response());
      return;
    }

    final String holdingsId = routingContext.request().getParam("id");
    final WebContext context = new WebContext(routingContext);
    final Map<String, JsonObject> recordsInTenant = getResourcesForTenant(context);

    if (recordsInTenant == null) {
      ClientErrorResponse.notFound(routingContext.response());
      return;
    }

    final Optional<JsonObject> foundRecord = recordsInTenant.values().stream()
      .filter(rec -> {
        JsonObject externalIdsHolder = rec.getJsonObject("externalIdsHolder");
        return externalIdsHolder != null && holdingsId.equals(externalIdsHolder.getString("holdingsId"));
      })
      .findFirst();

    if (foundRecord.isPresent()) {
      JsonResponse.success(routingContext.response(), foundRecord.get());
    } else {
      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void emulateFailure(RoutingContext routingContext) {
    endpointFailureDescriptor = routingContext.getBodyAsJson()
      .mapTo(EndpointFailureDescriptor.class);

    routingContext.response().setStatusCode(201).end();
  }
}

