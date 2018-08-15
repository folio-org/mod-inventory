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
      .put(Instance.TITLE, "dcterms:title"));

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

    if(StringUtils.isBlank(instanceRequest.getString(Instance.TITLE))) {
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
    representation.put(Instance.SOURCE, instance.source);
    representation.put(Instance.TITLE, instance.title);
    representation.put(Instance.ALTERNATIVE_TITLES, instance.alternativeTitles);
    putIfNotNull(representation, Instance.EDITION, instance.edition);
    representation.put(Instance.SERIES, instance.series);

    representation.put(Instance.IDENTIFIERS,
      new JsonArray(instance.identifiers.stream()
        .map(identifier -> new JsonObject()
          .put(Identifier.IDENTIFIER_TYPE_ID, identifier.identifierTypeId)
          .put(Identifier.VALUE, identifier.value))
        .collect(Collectors.toList())));

    representation.put(Instance.CONTRIBUTORS,
      new JsonArray(instance.contributors.stream()
        .map(contributor -> new JsonObject()
          .put(Contributor.CONTRIBUTOR_NAME_TYPE_ID, contributor.contributorNameTypeId)
          .put(Contributor.NAME, contributor.name)
          .put(Contributor.CONTRIBUTOR_TYPE_ID, contributor.contributorTypeId)
          .put(Contributor.CONTRIBUTOR_TYPE_TEXT, contributor.contributorTypeText))
        .collect(Collectors.toList())));

    representation.put(Instance.SUBJECTS, instance.subjects);
    if (instance.classifications != null)
      representation.put(Instance.CLASSIFICATIONS,
        new JsonArray(instance.classifications.stream()
          .map(classification -> new JsonObject()
            .put(Classification.CLASSIFICATION_TYPE_ID, classification.classificationTypeId)
            .put(Classification.CLASSIFICATION_NUMBER, classification.classificationNumber))
          .collect(Collectors.toList())));

    representation.put(Instance.PUBLICATION,
      new JsonArray(instance.publication.stream()
        .map(publication -> new JsonObject()
          .put(Publication.PUBLISHER, publication.publisher)
          .put(Publication.PLACE, publication.place)
          .put(Publication.DATE_OF_PUBLICATION, publication.dateOfPublication))
        .collect(Collectors.toList())));

    representation.put(Instance.URLS, instance.urls);
    representation.put(Instance.INSTANCE_TYPE_ID, instance.instanceTypeId);
    putIfNotNull(representation, Instance.INSTANCE_FORMAT_ID, instance.instanceFormatId);
    representation.put(Instance.PHYSICAL_DESCRIPTIONS, instance.physicalDescriptions);
    representation.put(Instance.LANGUAGES, instance.languages);
    representation.put(Instance.NOTES, instance.notes);
    putIfNotNull(representation, Instance.SOURCE_RECORD_FORMAT, instance.sourceRecordFormat);

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
    List<Identifier> identifiers = instanceRequest.containsKey(Instance.IDENTIFIERS)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.IDENTIFIERS)).stream()
          .map(identifier -> new Identifier(identifier.getString(Identifier.IDENTIFIER_TYPE_ID),
          identifier.getString(Identifier.VALUE)))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<Contributor> contributors = instanceRequest.containsKey(Instance.CONTRIBUTORS)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CONTRIBUTORS)).stream()
      .map(contributor -> new Contributor(contributor.getString(Contributor.CONTRIBUTOR_NAME_TYPE_ID),
        contributor.getString("name"), contributor.getString(Contributor.CONTRIBUTOR_TYPE_ID), contributor.getString(Contributor.CONTRIBUTOR_TYPE_TEXT)))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Classification> classifications = instanceRequest.containsKey(Instance.CLASSIFICATIONS)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CLASSIFICATIONS)).stream()
      .map(classification -> new Classification(classification.getString(Classification.CLASSIFICATION_TYPE_ID),
                                                classification.getString(Classification.CLASSIFICATION_NUMBER)))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Publication> publications = instanceRequest.containsKey(Instance.PUBLICATION)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.PUBLICATION)).stream()
      .map(publication -> new Publication(publication.getString(Publication.PUBLISHER),
                                          publication.getString(Publication.PLACE),
                                          publication.getString(Publication.DATE_OF_PUBLICATION)))
      .collect(Collectors.toList())
      : new ArrayList<>();

    return new Instance(
      instanceRequest.getString("id"),
      instanceRequest.getString(Instance.SOURCE),
      instanceRequest.getString(Instance.TITLE),
      instanceRequest.getString(Instance.INSTANCE_TYPE_ID))
      .setAlternativeTitles(jsonArrayAsListOfStrings(instanceRequest, Instance.ALTERNATIVE_TITLES))
      .setEdition(instanceRequest.getString(Instance.EDITION))
      .setSeries(jsonArrayAsListOfStrings(instanceRequest, Instance.SERIES))
      .setIdentifiers(identifiers)
      .setContributors(contributors)
      .setSubjects(jsonArrayAsListOfStrings(instanceRequest, Instance.SUBJECTS))
      .setClassifications(classifications)
      .setPublication(publications)
      .setUrls(jsonArrayAsListOfStrings(instanceRequest, Instance.URLS))
      .setInstanceFormatId(instanceRequest.getString(Instance.INSTANCE_FORMAT_ID))
      .setPhysicalDescriptions(jsonArrayAsListOfStrings(instanceRequest, Instance.PHYSICAL_DESCRIPTIONS))
      .setLanguages(jsonArrayAsListOfStrings(instanceRequest, Instance.LANGUAGES))
      .setNotes(jsonArrayAsListOfStrings(instanceRequest, Instance.NOTES))
      .setSourceRecordFormat(instanceRequest.getString(Instance.SOURCE_RECORD_FORMAT));
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
