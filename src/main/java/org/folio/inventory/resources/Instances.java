package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Instances {
  private final Storage storage;

  public Instances(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(relativeInstancesPath() + "*").handler(BodyHandler.create());
    router.put(relativeInstancesPath() + "*").handler(BodyHandler.create());

    router.get(relativeInstancesPath() + "/context")
      .handler(this::getMetadataContext);

    router.get(relativeInstancesPath()).handler(this::getAll);
    router.post(relativeInstancesPath()).handler(this::create);
    router.delete(relativeInstancesPath()).handler(this::deleteAll);

    router.get(relativeInstancesPath() + "/:id").handler(this::getById);
    router.put(relativeInstancesPath() + "/:id").handler(this::update);
    router.delete(relativeInstancesPath() + "/:id").handler(this::deleteById);
  }

  private void getMetadataContext(RoutingContext routingContext) {
    JsonObject representation = new JsonObject();

    representation.put("@context", new JsonObject()
      .put("dcterms", "http://purl.org/dc/terms/")
      .put("title", "dcterms:title"));

    JsonResponse.success(routingContext.response(), representation);
  }

  private void getAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String search = context.getStringParameter("query", null);

    PagingParameters pagingParameters = PagingParameters.from(context);

    if(pagingParameters == null) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "limit and offset must be numeric when supplied");

      return;
    }

    if(search == null) {
      storage.getInstanceCollection(context).findAll(
        pagingParameters,
        success -> JsonResponse.success(routingContext.response(),
          toRepresentation(success.getResult(), context)),
        FailureResponseConsumer.serverError(routingContext.response()));
    }
    else {
      try {
        storage.getInstanceCollection(context).findByCql(search,
          pagingParameters, success ->
            JsonResponse.success(routingContext.response(),
            toRepresentation(success.getResult(), context)),
          FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
    }
  }

  private void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject instanceRequest = routingContext.getBodyAsJson();

    if(StringUtils.isBlank(instanceRequest.getString("title"))) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Title must be provided for an instance");
      return;
    }

    Instance newInstance = requestToInstance(instanceRequest);

    storage.getInstanceCollection(context).add(newInstance,
      success -> {
        try {
          URL url = context.absoluteUrl(String.format("%s/%s",
            relativeInstancesPath(), success.getResult().id));

          RedirectResponse.created(routingContext.response(), url.toString());
        } catch (MalformedURLException e) {
          System.out.println(
            String.format("Failed to create self link for instance: " + e.toString()));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void update(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject instanceRequest = routingContext.getBodyAsJson();

    Instance updatedInstance = requestToInstance(instanceRequest);

    InstanceCollection instanceCollection = storage.getInstanceCollection(context);

    instanceCollection.findById(routingContext.request().getParam("id"),
      it -> {
        if(it.getResult() != null) {
          instanceCollection.update(updatedInstance,
            v -> SuccessResponse.noContent(routingContext.response()),
            FailureResponseConsumer.serverError(routingContext.response()));
        }
        else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).empty (
      v -> SuccessResponse.noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).delete(
      routingContext.request().getParam("id"),
      v -> SuccessResponse.noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void getById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).findById(
      routingContext.request().getParam("id"),
      it -> {
        if(it.getResult() != null) {
          JsonResponse.success(routingContext.response(),
            toRepresentation(it.getResult(), context));
        }
        else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private static String relativeInstancesPath() {
    return "/inventory/instances";
  }

  private JsonObject toRepresentation(
    MultipleRecords<Instance> wrappedInstances,
    WebContext context) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Instance> instances = wrappedInstances.records;

    instances.stream().forEach(instance ->
      results.add(toRepresentation(instance, context)));

    representation
      .put("instances", results)
      .put("totalRecords", wrappedInstances.totalRecords);

    return representation;
  }

  private JsonObject toRepresentation(Instance instance, WebContext context) {
    JsonObject representation = new JsonObject();

    try {
      representation.put("@context", context.absoluteUrl(
        relativeInstancesPath() + "/context").toString());
    } catch (MalformedURLException e) {
      System.out.println(String.format("Failed to create context link for instance: " + e.toString()));
    }

    representation.put("id", instance.id);
    representation.put("title", instance.title);

    representation.put("identifiers",
      new JsonArray(instance.identifiers.stream()
        .map(identifier -> { return new JsonObject()
          .put("namespace", identifier.namespace)
          .put("value", identifier.value); })
        .collect(Collectors.toList())));

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        relativeInstancesPath(), instance.id));

      representation.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      System.out.println(String.format("Failed to create self link for instance: " + e.toString()));
    }

    return representation;
  }

  private Instance requestToInstance(JsonObject instanceRequest) {
    List<Identifier> identifiers = instanceRequest.containsKey("identifiers")
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray("identifiers")).stream()
          .map(identifier -> new Identifier(identifier.getString("namespace"),
          identifier.getString("value")))
          .collect(Collectors.toList())
          : new ArrayList<>();

    return new Instance(instanceRequest.getString("id"), instanceRequest.getString("title"),
      identifiers);
  }
}
