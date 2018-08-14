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
import org.folio.inventory.domain.Classification;
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
import org.folio.inventory.domain.Publication;

public class Instances {
  private static final String INSTANCES_PATH = "/inventory/instances";
  private static final String TITLE_PROPERTY_NAME = "title";
  private static final String IDENTIFIER_PROPERTY_NAME = "identifiers";
  private static final String CONTRIBUTORS_PROPERTY_NAME = "contributors";
  private static final String CLASSIFICATIONS_PROPERTY_NAME = "classifications";
  private static final String PUBLICATION_PROPERTY_NAME = "publication";

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

    representation.put("id", instance.id);
    representation.put("source", instance.source);
    representation.put(TITLE_PROPERTY_NAME, instance.title);
    representation.put("alternativeTitles", instance.alternativeTitles);
    putIfNotNull(representation, "edition", instance.edition);
    representation.put("series", instance.series);

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
          .put("name", contributor.name)
          .put("contributorTypeId", contributor.contributorTypeId)
          .put("contributorTypeText", contributor.contributorTypeText))
        .collect(Collectors.toList())));

    representation.put("subjects", instance.subjects);

    representation.put(CLASSIFICATIONS_PROPERTY_NAME,
      new JsonArray(instance.classifications.stream()
        .map(classification -> new JsonObject()
          .put("classificationTypeId", classification.classificationTypeId)
          .put("classificationNumber", classification.classificationNumber))
        .collect(Collectors.toList())));

    representation.put(PUBLICATION_PROPERTY_NAME,
      new JsonArray(instance.publications.stream()
        .map(publication -> new JsonObject()
          .put("publisher", publication.publisher)
          .put("place", publication.place)
          .put("dateOfPublication", publication.dateOfPublication))
        .collect(Collectors.toList())));

    representation.put("urls", instance.urls);
    representation.put("instanceTypeId", instance.instanceTypeId);
    putIfNotNull(representation, "instanceFormatId", instance.instanceFormatId);
    representation.put("physicalDescriptions", instance.physicalDescriptions);
    representation.put("languages", instance.languages);
    representation.put("notes", instance.notes);
    putIfNotNull(representation, "sourceRecordFormat", instance.sourceRecordFormat);

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
    List<Identifier> identifiers = instanceRequest.containsKey(IDENTIFIER_PROPERTY_NAME)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(IDENTIFIER_PROPERTY_NAME)).stream()
          .map(identifier -> new Identifier(identifier.getString("identifierTypeId"),
          identifier.getString("value")))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<Contributor> contributors = instanceRequest.containsKey(CONTRIBUTORS_PROPERTY_NAME)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(CONTRIBUTORS_PROPERTY_NAME)).stream()
      .map(contributor -> new Contributor(contributor.getString("contributorNameTypeId"),
        contributor.getString("name"), contributor.getString("contributorTypeId"), contributor.getString("contributorTypeText")))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Classification> classifications = instanceRequest.containsKey(CLASSIFICATIONS_PROPERTY_NAME)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(CLASSIFICATIONS_PROPERTY_NAME)).stream()
      .map(classification -> new Classification(classification.getString("classificationTypeId"),
                                                classification.getString("classificationNumber")))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Publication> publications = instanceRequest.containsKey(PUBLICATION_PROPERTY_NAME)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(PUBLICATION_PROPERTY_NAME)).stream()
      .map(publication -> new Publication(publication.getString("publisher"),
                                          publication.getString("place"),
                                          publication.getString("dateOfPublication")))
      .collect(Collectors.toList())
      : new ArrayList<>();

    return new Instance(
      instanceRequest.getString("id"),
      instanceRequest.getString("source"),
      instanceRequest.getString(TITLE_PROPERTY_NAME),
      instanceRequest.getString("instanceTypeId"))
      .setAlternativeTitles(jsonArrayAsListOfStrings(instanceRequest, "alternativeTitles"))
      .setEdition(instanceRequest.getString("edition"))
      .setSeries(jsonArrayAsListOfStrings(instanceRequest, "series"))
      .setIdentifiers(identifiers)
      .setContributors(contributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceRequest, "subjects"))
      .setClassifications(classifications)
      .setPublication(publications)
      .setUrls(jsonArrayAsListOfStrings(instanceRequest, "urls"))
      .setInstanceFormatId(instanceRequest.getString("instanceFormatId"))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceRequest, "physicalDescriptions"))
      .setLanguages(jsonArrayAsListOfStrings(instanceRequest, "languages"))
      .setNotes(jsonArrayAsListOfStrings(instanceRequest, "notes"))
      .setSourceRecordFormat(instanceRequest.getString("sourceRecordFormat"));
  }

  private void putIfNotNull (JsonObject target, String propertyName, String value) {
    if (value != null) target.put(propertyName, value);
  }

  private List<String> jsonArrayAsListOfStrings(JsonObject source, String propertyName) {
    return source.containsKey(propertyName)
      ? JsonArrayHelper.toListOfStrings(source.getJsonArray(propertyName))
      : new ArrayList<>();
  }
}
