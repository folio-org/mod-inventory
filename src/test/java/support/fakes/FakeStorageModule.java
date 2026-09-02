package support.fakes;

import static api.ApiTestSuite.ID_FOR_FAILURE;
import static api.ApiTestSuite.ID_FOR_OPTIMISTIC_LOCKING_FAILURE;
import static java.util.concurrent.CompletableFuture.completedFuture;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.EndpointFailureHandler;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.joda.time.DateTime;
import support.fakes.processors.RecordPreProcessor;

class FakeStorageModule extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger(FakeOkapi.class);

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

  FakeStorageModule(String rootPath,
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

    Map<String, Supplier<Object>> defaultPropertiesWithId = new HashMap<>(defaultProperties);
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
    router.patch(pathTree).handler(BodyHandler.create());

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

    router.patch(rootPath + "/:id").handler(this::patch);

    router.get(rootPath + "/:id").handler(this::get);
    router.delete(rootPath + "/:id").handler(this::delete);

    router.get(rootPath + "/:id/formatted").handler(this::getByExternalId);
    router.post("/source-storage/snapshots").handler(this::createSnapshot);
  }

  void registerBatch(Router router, String batchPath) {
    String pathTree = batchPath + "/*";

    router.post(pathTree).handler(BodyHandler.create());
    router.post(batchPath).handler(BodyHandler.create());
    router.route(batchPath).handler(this::checkTokenHeader);
    router.post(batchPath).handler(this::createBatch);
  }

  private void emulateFailureIfNeeded(RoutingContext routingContext) {
    if (!shouldEmulateFailure(routingContext)) {
      routingContext.next();
      return;
    }

    String urlPattern = endpointFailureDescriptor.getUrlPattern();
    if (StringUtils.isNotBlank(urlPattern) && !routingContext.request().uri().matches(urlPattern)) {
      routingContext.next();
      return;
    }

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

      LOGGER.info("Created {} resource: {}", recordTypeName, id);
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
      LOGGER.info("Created {} resource: {}", recordTypeName, id);

      JsonResponse.created(routingContext.response(), body);
    }).exceptionally(error -> {
      EndpointFailureHandler.handleFailure(EndpointFailureHandler.getKnownException(error),
        routingContext);

      return null;
    });
  }

  private CompletableFuture<Void> createElement(WebContext context, JsonObject rawBody) {
    String id = rawBody.getString("id");

    return preProcessRecords(context.getTenantId(), null, rawBody).thenAccept(
      body -> getResourcesForTenant(context).put(id, body));
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
        LOGGER.info("Replaced {} resource: {}", recordTypeName, id);

        resourcesForTenant.replace(id, body);
        SuccessResponse.noContent(routingContext.response());
      } else {
        LOGGER.info("Created {} resource: {}", recordTypeName, id);

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

      LOGGER.info("Found {} resource: {}", recordTypeName,
        resourceRepresentation.encodePrettily());

      JsonResponse.success(routingContext.response(), resourceRepresentation);
    } else {
      LOGGER.info("Failed to find {} resource: {}", recordTypeName, id);

      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void getMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    Integer limit = context.getIntegerParameter("limit", 10);
    Integer offset = context.getIntegerParameter("offset", 0);
    String query = context.getStringParameter("query", null);

    LOGGER.info("Handling {}", routingContext.request().uri());

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    List<JsonObject> filteredItems = new FakeCQLToJSONInterpreter(false)
      .execute(resourcesForTenant.values(), query);

    List<JsonObject> pagedItems = filteredItems.stream()
      .skip(offset)
      .limit(limit)
      .toList();

    JsonObject result = new JsonObject();

    result.put(collectionPropertyName, new JsonArray(pagedItems));
    result.put("totalRecords", filteredItems.size());

    LOGGER.info("Found {} resources: {}", recordTypeName,
      result.encodePrettily());

    JsonResponse.success(routingContext.response(), result);
  }

  private void retrieveMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    var requestBody = routingContext.body().asJsonObject();

    var limit = requestBody.getInteger("limit");
    var offset = requestBody.getInteger("offset");
    var query = requestBody.getString("query");

    LOGGER.info("Handling {}", routingContext.request().uri());

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    List<JsonObject> filteredItems = new FakeCQLToJSONInterpreter(false)
      .execute(resourcesForTenant.values(), query);

    List<JsonObject> pagedItems = filteredItems.stream()
      .skip(offset)
      .limit(limit)
      .toList();

    JsonObject result = new JsonObject();

    result.put(collectionPropertyName, new JsonArray(pagedItems));
    result.put("totalRecords", filteredItems.size());

    LOGGER.info("Found {} resources: {}", recordTypeName,
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
      LOGGER.info("Deleted {} resource: {}", recordTypeName, id);
      resourcesForTenant.remove(id);

      SuccessResponse.noContent(routingContext.response());
    } else {
      LOGGER.info("{} resource: {} for deletion is not found", recordTypeName, id);
      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private Map<String, JsonObject> getResourcesForTenant(WebContext context) {
    return storedResourcesByTenant.get(context.getTenantId());
  }

  private static JsonObject getJsonFromBody(RoutingContext routingContext) {
    if (hasBody(routingContext)) {
      return routingContext.body().asJsonObject();
    } else {
      return new JsonObject();
    }
  }

  private static boolean hasBody(RoutingContext routingContext) {
    return StringUtils.isNotBlank(routingContext.body().asString());
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
        .map(jsonObject -> jsonObject.getString(uniqueProperty))
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
    endpointFailureDescriptor = routingContext.body().asJsonObject()
      .mapTo(EndpointFailureDescriptor.class);

    routingContext.response().setStatusCode(201).end();
  }

  private void patch(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);
    if (resourcesForTenant == null || !resourcesForTenant.containsKey(id)) {
      ClientErrorResponse.notFound(routingContext.response());
      return;
    }

    JsonObject patchBody = getJsonFromBody(routingContext);
    JsonObject existing = resourcesForTenant.get(id);

    JsonObject merged = existing.copy();
    deepMergeInto(merged, patchBody);

    preProcessRecords(context.getTenantId(), existing, merged).thenAccept(processed -> {
      setDefaultProperties(processed);
      resourcesForTenant.replace(id, processed);
      SuccessResponse.noContent(routingContext.response());
    }).exceptionally(error -> {
      EndpointFailureHandler.handleFailure(EndpointFailureHandler.getKnownException(error),
        routingContext);
      return null;
    });
  }

  private static void deepMergeInto(JsonObject target, JsonObject patch) {
    if (patch == null) { return; }

    for (String key : patch.fieldNames()) {
      Object patchVal = patch.getValue(key);

      if (patchVal instanceof JsonObject patchObj) {
        Object existingVal = target.getValue(key);
        if (existingVal instanceof JsonObject existingObj) {
          deepMergeInto(existingObj, patchObj);
          target.put(key, existingObj);
        } else {
          target.put(key, patchObj.copy());
        }
      } else {
        target.put(key, patchVal);
      }
    }
  }
}

