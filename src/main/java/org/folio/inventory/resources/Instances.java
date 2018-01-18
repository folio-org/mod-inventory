package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.Contributor;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.inventory.domain.Instance.CONTRIBUTORS_PROPERTY_NAME;
import static org.folio.inventory.domain.Instance.ID_NAME;
import static org.folio.inventory.domain.Instance.IDENTIFIER_PROPERTY_NAME;
import static org.folio.inventory.domain.Instance.INSTANCE_TYPE_ID_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_BINARY_BASE64_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_BINARY_FORMAT_NAME;
import static org.folio.inventory.domain.Instance.SOURCE_NAME;
import static org.folio.inventory.domain.Instance.TITLE_PROPERTY_NAME;

public class Instances {
  private static final String INSTANCES_PATH = "/inventory/instances";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Storage storage;

  public Instances(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.put(INSTANCES_PATH + "*").handler(BodyHandler.create());

    router.get(INSTANCES_PATH + "/context")
      .handler(this::getMetadataContext);

    router.get(INSTANCES_PATH).handler(this::getAll);
    router.post(INSTANCES_PATH).handler(this::create);
    router.delete(INSTANCES_PATH).handler(this::deleteAll);

    router.get(INSTANCES_PATH + "/:id").handler(this::getById);
    router.put(INSTANCES_PATH + "/:id").handler(this::update);
    router.delete(INSTANCES_PATH + "/:id").handler(this::deleteById);
  }

  private void getMetadataContext(RoutingContext routingContext) {
    JsonObject representation = new JsonObject();

    representation.put("@context", new JsonObject()
      .put("dcterms", "http://purl.org/dc/terms/")
      .put(TITLE_PROPERTY_NAME, "dcterms:title"));

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

    if(StringUtils.isBlank(instanceRequest.getString(TITLE_PROPERTY_NAME))) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Title must be provided for an instance");
      return;
    }

    Instance newInstance = requestToInstance(instanceRequest);

    storage.getInstanceCollection(context).add(newInstance,
      success -> {
        try {
          URL url = context.absoluteUrl(String.format("%s/%s",
            INSTANCES_PATH, success.getResult().id));

          RedirectResponse.created(routingContext.response(), url.toString());
        } catch (MalformedURLException e) {
          log.warn(
            String.format("Failed to create self link for instance: %s", e.toString()));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void update(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject instanceRequest = routingContext.getBodyAsJson();

    Instance updatedInstance = requestToInstance(instanceRequest);

    InstanceCollection instanceCollection = storage.getInstanceCollection(context);

    instanceCollection.findById(routingContext.request().getParam(ID_NAME),
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
      routingContext.request().getParam(ID_NAME),
      v -> SuccessResponse.noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void getById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).findById(
      routingContext.request().getParam(ID_NAME),
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
        INSTANCES_PATH + "/context").toString());
    } catch (MalformedURLException e) {
      log.warn(
        String.format("Failed to create context link for instance: %s", e.toString()));
    }

    representation.put(ID_NAME, instance.id);
    representation.put(TITLE_PROPERTY_NAME, instance.title);
    representation.put(SOURCE_NAME, instance.source);
    representation.put(SOURCE_BINARY_BASE64_NAME, instance.sourceBinaryBase64);
    representation.put(SOURCE_BINARY_FORMAT_NAME, instance.sourceBinaryFormat);
    representation.put(INSTANCE_TYPE_ID_NAME, instance.instanceTypeId);

    representation.put(IDENTIFIER_PROPERTY_NAME,
      new JsonArray(instance.identifiers.stream()
        .map(identifier -> new JsonObject()
          .put("identifierTypeId", identifier.identifierTypeId)
          .put("value", identifier.value))
        .collect(Collectors.toList())));

    representation.put(CONTRIBUTORS_PROPERTY_NAME,
      new JsonArray(instance.contributors.stream()
        .map(contributor -> new JsonObject()
          .put("contributorNameTypeId", contributor.contributorNameTypeId)
          .put("name", contributor.name))
        .collect(Collectors.toList())));

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        INSTANCES_PATH, instance.id));

      representation.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      log.warn(
        String.format("Failed to create self link for instance: %s", e.toString()));
    }

    return representation;
  }

  private Instance requestToInstance(JsonObject instanceRequest) {
    List<Identifier> identifiers =
      JsonArrayHelper.toList(instanceRequest.getJsonArray(IDENTIFIER_PROPERTY_NAME))
        .stream()
        .map(identifier -> new Identifier(identifier.getString("identifierTypeId"),
            identifier.getString("value")))
        .collect(Collectors.toCollection(ArrayList::new));  // modifiable List

    List<Contributor> contributors =
      JsonArrayHelper.toList(instanceRequest.getJsonArray(CONTRIBUTORS_PROPERTY_NAME))
        .stream()
        .map(contributor -> new Contributor(contributor.getString("contributorNameTypeId"),
            contributor.getString("name")))
        .collect(Collectors.toCollection(ArrayList::new));  // modifiable List

    return new Instance(
      instanceRequest.getString(ID_NAME),
      instanceRequest.getString(TITLE_PROPERTY_NAME),
      identifiers,
      instanceRequest.getString(SOURCE_NAME),
      instanceRequest.getString(INSTANCE_TYPE_ID_NAME),
      contributors,
      instanceRequest.getString(SOURCE_BINARY_BASE64_NAME),
      instanceRequest.getString(SOURCE_BINARY_FORMAT_NAME) );
  }
}
