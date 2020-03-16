package support.fakes;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.joda.time.DateTime;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

class FakeStorageModule extends AbstractVerticle {
  private final String rootPath;
  private final String collectionPropertyName;
  private final boolean hasCollectionDelete;
  private final Collection<String> requiredProperties;
  private final Map<String, Map<String, JsonObject>> storedResourcesByTenant;
  private final String recordTypeName;
  private final Collection<String> uniqueProperties;
  private final Map<String, Supplier<Object>> defaultProperties;
  private final List<BiFunction<JsonObject, JsonObject, CompletableFuture<JsonObject>>> recordPreProcessors;
  private EndpointFailureDescriptor endpointFailureDescriptor = null;

  FakeStorageModule(
    String rootPath,
    String collectionPropertyName,
    String tenantId,
    Collection<String> requiredProperties,
    boolean hasCollectionDelete,
    String recordTypeName,
    Collection<String> uniqueProperties,
    Map<String, Supplier<Object>> defaultProperties,
    List<BiFunction<JsonObject, JsonObject, CompletableFuture<JsonObject>>> recordPreProcessors) {

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
    storedResourcesByTenant.put(tenantId, new HashMap<>());
    this.recordPreProcessors = recordPreProcessors;
  }

  void register(Router router) {
    String pathTree = rootPath + "/*";

    router.route(pathTree).handler(this::emulateFailureIfNeeded);
    router.route(pathTree).handler(this::checkTokenHeader);

    router.post(pathTree).handler(BodyHandler.create());
    router.put(pathTree).handler(BodyHandler.create());

    router.post(rootPath).handler(this::checkRequiredProperties);
    router.post(rootPath).handler(this::checkUniqueProperties);
    router.post(rootPath).handler(this::create);

    router.get(rootPath).handler(this::getMany);
    router.delete(rootPath).handler(this::empty);

    router.put(rootPath + "/:id").handler(this::checkRequiredProperties);
    router.put(rootPath + "/:id").handler(this::replace);

    router.get(rootPath + "/:id").handler(this::get);
    router.delete(rootPath + "/:id").handler(this::delete);
    router.post(rootPath + "/emulate-failure").handler(this::emulateFailure);
  }

  private void emulateFailureIfNeeded(RoutingContext routingContext) {
    if (shouldEmulateFailure(routingContext)) {
      final String body = endpointFailureDescriptor.getBody();

      routingContext.response()
        .setStatusCode(endpointFailureDescriptor.getStatusCode())
        .putHeader(HttpHeaders.CONTENT_TYPE, endpointFailureDescriptor.getContentType())
        .putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(body.length()))
        .write(body)
        .end();
    } else {
      routingContext.next();
    }
  }

  private boolean shouldEmulateFailure(RoutingContext routingContext) {
    if (routingContext.request().uri().endsWith("/emulate-failure")) {
      return false;
    }

    return endpointFailureDescriptor != null && DateTime.now().toDate()
      .before(endpointFailureDescriptor.getFailureExpireDate());
  }

  void registerBatch(Router router, String batchPath) {
    String pathTree = batchPath + "/*";

    router.post(pathTree).handler(BodyHandler.create());
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

      System.out.println(
        String.format("Created %s resource: %s", recordTypeName, id));
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
      System.out.println(
        String.format("Created %s resource: %s", recordTypeName, id));

      JsonResponse.created(routingContext.response(), body);
    });
  }

  private CompletableFuture<Void> createElement(WebContext context, JsonObject rawBody) {
    String id = rawBody.getString("id");

    return preProcessRecords(null, rawBody).thenAccept(body -> {
      getResourcesForTenant(context).put(id, body);
    });
  }

  private void replace(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    JsonObject rawBody = getJsonFromBody(routingContext);
    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    preProcessRecords(resourcesForTenant.get(id), rawBody).thenAccept(body -> {
      setDefaultProperties(body);

      if (resourcesForTenant.containsKey(id)) {
        System.out.println(
          String.format("Replaced %s resource: %s", recordTypeName, id));

        resourcesForTenant.replace(id, body);
        SuccessResponse.noContent(routingContext.response());
      } else {
        System.out.println(
          String.format("Created %s resource: %s", recordTypeName, id));

        resourcesForTenant.put(id, body);
        SuccessResponse.noContent(routingContext.response());
      }
    });
  }

  private void get(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if(resourcesForTenant.containsKey(id)) {
      final JsonObject resourceRepresentation = resourcesForTenant.get(id);

      System.out.println(
        String.format("Found %s resource: %s", recordTypeName,
          resourceRepresentation.encodePrettily()));

      JsonResponse.success(routingContext.response(), resourceRepresentation);
    }
    else {
      System.out.println(
        String.format("Failed to find %s resource: %s", recordTypeName, id));

      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void getMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    Integer limit = context.getIntegerParameter("limit", 10);
    Integer offset = context.getIntegerParameter("offset", 0);
    String query = context.getStringParameter("query", null);

    System.out.println(String.format("Handling %s", routingContext.request().uri()));

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

    System.out.println(
      String.format("Found %s resources: %s", recordTypeName,
        result.encodePrettily()));

    JsonResponse.success(routingContext.response(), result);
  }

  private void empty(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    if(!hasCollectionDelete) {
      ClientErrorResponse.notFound(routingContext.response());
      return;
    }

    Map <String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    resourcesForTenant.clear();

    SuccessResponse.noContent(routingContext.response());
  }

  private void delete(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if(resourcesForTenant.containsKey(id)) {
      resourcesForTenant.remove(id);

      SuccessResponse.noContent(routingContext.response());
    }
    else {
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

    if(StringUtils.isBlank(context.getToken())) {
      ClientErrorResponse.forbidden(routingContext.response());
    }
    else {
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
    if(uniqueProperties.isEmpty()) {
      routingContext.next();
      return;
    }

    JsonObject body = getJsonFromBody(routingContext);

    ArrayList<ValidationError> errors = new ArrayList<>();

    uniqueProperties.stream().forEach(uniqueProperty -> {
      String proposedValue = body.getString(uniqueProperty);

      Map<String, JsonObject> records = getResourcesForTenant(new WebContext(routingContext));

      if(records.values().stream()
        .map(record -> record.getString(uniqueProperty))
        .anyMatch(usedValue -> usedValue.equals(proposedValue))) {

        errors.add(new ValidationError(
          String.format("%s with this %s already exists", recordTypeName, uniqueProperty),
          uniqueProperty, proposedValue));

        JsonResponse.unprocessableEntity(routingContext.response(),
          errors);
      }
    });

    if(errors.isEmpty()) {
      routingContext.next();
    }
  }

  private void setDefaultProperties(JsonObject representation) {
    defaultProperties.forEach((property, valueSupplier) -> {
      if(!representation.containsKey(property)) {
        representation.put(property, valueSupplier.get());
      }
    });
  }

  private CompletableFuture<JsonObject> preProcessRecords(JsonObject oldBody, JsonObject newBody) {
    CompletableFuture<JsonObject> lastPreProcess = completedFuture(newBody);

    for (BiFunction<JsonObject, JsonObject, CompletableFuture<JsonObject>> preProcessor : recordPreProcessors) {
      lastPreProcess = lastPreProcess
        .thenCompose(prev -> preProcessor.apply(oldBody, newBody));
    }

    return lastPreProcess;
  }

  private void emulateFailure(RoutingContext routingContext) {
    endpointFailureDescriptor = routingContext.getBodyAsJson()
      .mapTo(EndpointFailureDescriptor.class);

    routingContext.response().setStatusCode(201).end();
  }
}

