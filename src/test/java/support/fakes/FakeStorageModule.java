package support.fakes;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FakeStorageModule extends AbstractVerticle {
  private final String rootPath;
  private final Collection<String> requiredProperties;

  private final Map<String, Map<String, JsonObject>> storedResourcesByTenant;
  private final String collectionPropertyName;

  public FakeStorageModule(
    String rootPath,
    String collectionPropertyName,
    String tenantId) {

    this(rootPath, collectionPropertyName, tenantId, new ArrayList<>());
  }

  public FakeStorageModule(
    String rootPath,
    String collectionPropertyName,
    String tenantId,
    Collection<String> requiredProperties) {

    this.rootPath = rootPath;
    this.collectionPropertyName = collectionPropertyName;
    this.requiredProperties = requiredProperties;

    storedResourcesByTenant = new HashMap<>();
    storedResourcesByTenant.put(tenantId, new HashMap<>());
  }

  public void register(Router router) {

    router.route().handler(this::checkTokenHeader);

    router.post(rootPath + "*").handler(BodyHandler.create());
    router.put(rootPath + "*").handler(BodyHandler.create());

    router.post(rootPath).handler(this::checkRequiredProperties);
    router.post(rootPath).handler(this::create);

    router.get(rootPath).handler(this::getMany);
    router.delete(rootPath).handler(this::empty);

    router.put(rootPath + "/:id").handler(this::checkRequiredProperties);
    router.put(rootPath + "/:id").handler(this::replace);

    router.get(rootPath + "/:id").handler(this::get);
    router.delete(rootPath + "/:id").handler(this::delete);
  }

  private void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject body = getJsonFromBody(routingContext);

    String id = body.getString("id", UUID.randomUUID().toString());

    body.put("id", id);

    getResourcesForTenant(context).put(id, body);

    JsonResponse.created(routingContext.response(), body);
  }

  private void replace(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    JsonObject body = getJsonFromBody(routingContext);

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if(resourcesForTenant.containsKey(id)) {
      resourcesForTenant.replace(id, body);
      SuccessResponse.noContent(routingContext.response());
    }
    else {
      resourcesForTenant.put(id, body);
      SuccessResponse.noContent(routingContext.response());
    }
  }

  private void get(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String id = routingContext.request().getParam("id");

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    if(resourcesForTenant.containsKey(id)) {
      JsonResponse.success(routingContext.response(),
        resourcesForTenant.get(id));
    }
    else {
      ClientErrorResponse.notFound(routingContext.response());
    }
  }

  private void getMany(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    Integer limit = context.getIntegerParameter("limit", 10);
    Integer offset = context.getIntegerParameter("offset", 0);
    String query = context.getStringParameter("query", null);

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

    List<Predicate<JsonObject>> predicates = filterFromQuery(query);

    List<JsonObject> filteredItems = resourcesForTenant.values().stream()
      .filter(predicates.stream().reduce(Predicate::and).orElse(t -> false))
      .collect(Collectors.toList());

    List<JsonObject> pagedItems = filteredItems.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());

    JsonObject result = new JsonObject();

    result.put(collectionPropertyName, new JsonArray(pagedItems));
    result.put("totalRecords", filteredItems.size());

    JsonResponse.success(routingContext.response(), result);
  }

  private void empty(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    Map<String, JsonObject> resourcesForTenant = getResourcesForTenant(context);

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
    return routingContext.getBodyAsString() != null &&
      routingContext.getBodyAsString().trim() != "";
  }

  private List<Predicate<JsonObject>> filterFromQuery(String query) {

    if(query == null || query.trim() == "") {
      ArrayList<Predicate<JsonObject>> predicates = new ArrayList<>();
      predicates.add(t -> true);
      return predicates;
    }

    List<ImmutableTriple<String, String, String>> pairs =
      Arrays.stream(query.split(" and "))
        .map( pairText -> {
          String[] split = pairText.split("=|<>");
          String searchField = split[0];

          String searchTerm = split[1]
            .replaceAll("\"", "")
            .replaceAll("\\*", "");

          if(pairText.contains("=")) {
            return new ImmutableTriple<>(searchField, searchTerm, "=");
          }
          else {
            return new ImmutableTriple<>(searchField, searchTerm, "<>");
          }
        })
        .collect(Collectors.toList());

    return pairs.stream()
      .map(pair -> filterByField(pair.getLeft(), pair.getMiddle(), pair.getRight()))
      .collect(Collectors.toList());
  }

  private Predicate<JsonObject> filterByField(String field, String term, String operator) {
    return loan -> {
      if (term == null || field == null) {
        return true;
      } else {

        String propertyValue = "";

        //TODO: Should bomb if property does not exist
        if(field.contains(".")) {
          String[] fields = field.split("\\.");

          propertyValue = loan.getJsonObject(String.format("%s", fields[0]))
            .getString(String.format("%s", fields[1]));
        }
        else {
          propertyValue = loan.getString(String.format("%s", field));
        }

        switch(operator) {
          case "=":
            return propertyValue.contains(term);
          case "<>":
            return !propertyValue.contains(term);
          default:
            return false;
        }
      }
    };
  }

  private void checkTokenHeader(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    if(context.getToken() == null || context.getToken() == "") {
      ClientErrorResponse.forbidden(routingContext.response());
    }
    else {
      routingContext.next();
    }
  }

  private void checkRequiredProperties(RoutingContext routingContext) {
    JsonObject body = getJsonFromBody(routingContext);

    ArrayList<ValidationError> errors = new ArrayList<>();

    requiredProperties.stream().forEach(requiredProperty -> {
      if(!body.getMap().containsKey(requiredProperty)) {
        errors.add(new ValidationError(requiredProperty, null));
      }
    });

    if(errors.isEmpty()) {
      routingContext.next();
    }
    else {
      JsonResponse.unprocessableEntity(routingContext.response(),
        "Missing required properties", errors);
    }
  }
}
