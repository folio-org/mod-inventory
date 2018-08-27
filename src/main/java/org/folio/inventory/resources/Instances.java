package org.folio.inventory.resources;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

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
      .put(Instance.TITLE_KEY, "dcterms:title"));

    JsonResponse.success(routingContext.response(), representation);
  }

  private void getAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String search = context.getStringParameter("query", null);

    PagingParameters pagingParameters = PagingParameters.from(context);

    if (pagingParameters == null) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "limit and offset must be numeric when supplied");

      return;
    }

    if (search == null) {
      storage.getInstanceCollection(context).findAll(
        pagingParameters,
        (Success<MultipleRecords<Instance>> success) -> {
          buildInstancesResponse(success, routingContext, context);
        },
        FailureResponseConsumer.serverError(routingContext.response())
      );
    } else {
      try {
        storage.getInstanceCollection(context).findByCql(
          search,
          pagingParameters,
          success -> {
            buildInstancesResponse(success, routingContext, context);
          },
          FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
    }
  }

  private void buildInstancesResponse(Success<MultipleRecords<Instance>> success,
          RoutingContext routingContext,
          WebContext context) {
    List<String> instanceIds = success.getResult().records.stream()
            .map(instance -> instance.getId())
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());
    String idList = instanceIds.stream().map(String::toString).distinct().collect(Collectors.joining(" or "));

    String query = String.format("(subInstanceId==(%s)+or+superInstanceId==(%s))", idList, idList);

    try {
      query = URLEncoder.encode(query, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error(String.format("Cannot encode query %s", query));
    }
    CollectionResourceClient instancesIdsClient = null;

    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
      instancesIdsClient
              = new CollectionResourceClient(
                      okapiClient,
                      new URL(context.getOkapiLocation() + "/instance-storage/instance-relationships"));
    } catch (MalformedURLException mfue) {
      log.info(mfue);
    }

    Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap();
    Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap();
    if (instancesIdsClient != null) {
      CompletableFuture<Response> parentInstancesFetched = new CompletableFuture<>();
      instancesIdsClient.getMany(query, (Response result) -> {
        if (result.getStatusCode() == 200) {
          JsonObject json = result.getJson();
          List<JsonObject> relationsList = JsonArrayHelper.toList(json.getJsonArray("instanceRelationships"));
          relationsList.stream().map((rel) -> {
            addToList(childInstanceMap, rel.getString("superInstanceId"), new InstanceRelationshipToChild(rel));
            return rel;
          }).forEachOrdered((rel) -> {
            addToList(parentInstanceMap, rel.getString("subInstanceId"), new InstanceRelationshipToParent(rel));
          });
        } else {
          log.info("No relations found for instance ID list");
        }
        JsonResponse.success(routingContext.response(),
                toRepresentation(success.getResult(), parentInstanceMap, childInstanceMap, context));
      });
    }
  }

  private synchronized void addToList(Map<String, List<InstanceRelationshipToChild>> items, String mapKey, InstanceRelationshipToChild myItem) {
    List<InstanceRelationshipToChild> itemsList = items.get(mapKey);

    // if list does not exist create it
    if (itemsList == null) {
      itemsList = new ArrayList();
      itemsList.add(myItem);
      items.put(mapKey, itemsList);
    } else {
      // add if item is not already in list
      if (!itemsList.contains(myItem)) {
        itemsList.add(myItem);
      }
    }
  }

  private synchronized void addToList(Map<String, List<InstanceRelationshipToParent>> items, String mapKey, InstanceRelationshipToParent myItem) {
    List<InstanceRelationshipToParent> itemsList = items.get(mapKey);

    // if list does not exist create it
    if (itemsList == null) {
      itemsList = new ArrayList();
      itemsList.add(myItem);
      items.put(mapKey, itemsList);
    } else {
      // add if item is not already in list
      if (!itemsList.contains(myItem)) {
        itemsList.add(myItem);
      }
    }
  }

  private void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject instanceRequest = routingContext.getBodyAsJson();

    if (StringUtils.isBlank(instanceRequest.getString(Instance.TITLE_KEY))) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Title must be provided for an instance");
      return;
    }

    Instance newInstance = requestToInstance(instanceRequest);

    storage.getInstanceCollection(context).add(newInstance,
      success -> {
        try {
          URL url = context.absoluteUrl(String.format("%s/%s",
            INSTANCES_PATH, success.getResult().getId()));

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
        if (it.getResult() != null) {
          instanceCollection.update(updatedInstance,
            v -> SuccessResponse.noContent(routingContext.response()),
            FailureResponseConsumer.serverError(routingContext.response()));
        } else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).empty(
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
              if (it.getResult() != null) {
                List<String> instanceIds = Arrays.asList(it.getResult().getId());
                log.info("Gathered instance ID list: " + instanceIds);
                String idList = instanceIds.stream().map(String::toString).distinct().collect(Collectors.joining(" or "));
                String query = String.format(
                        "query=(subInstanceId==(%s)+or+superInstanceid==(%s))", idList, idList);
                //try {
                //query = URLEncoder.encode(query, "UTF-8");
                log.info("Created query: " + query);

                //} catch (UnsupportedEncodingException e) {
                //  log.error(String.format("Cannot encode query %s", query));
                //}
                CollectionResourceClient parentInstancesIdsClient = null;
                try {
                  OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
                  parentInstancesIdsClient
                  = new CollectionResourceClient(
                          okapiClient,
                          new URL(context.getOkapiLocation() + "/instance-storage/instance-relationships"));
                } catch (MalformedURLException mfue) {
                  log.info(mfue);
                }
                Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap();
                Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap();
                if (parentInstancesIdsClient != null) {
                  CompletableFuture<Response> parentInstancesFetched = new CompletableFuture<>();
                  log.info("Getting related instances");
                  parentInstancesIdsClient.getMany(query, (Response result) -> {
                    log.info("Sent off");
                    if (result.getStatusCode() == 200) {

                      JsonObject json = result.getJson();
                      log.info("Got json: " + json);
                      final List<JsonObject> relationsList = JsonArrayHelper.toList(
                              json.getJsonArray("instanceRelationships"));
                      // new Hashmap<String subInstanceId, List<InstanceRelationshipToParent>>();
                      for (JsonObject rel : relationsList) {
                        if (parentInstanceMap.containsKey(rel.getString("subInstanceId"))) {
                          parentInstanceMap.get(rel.getString("subInstanceId")).add(new InstanceRelationshipToParent(rel));
                        } else {
                          parentInstanceMap.put(rel.getString("subInstanceId"), Arrays.asList(new InstanceRelationshipToParent(rel)));
                        }
                        if (childInstanceMap.containsKey(rel.getString("superInstanceId"))) {
                          childInstanceMap.get(rel.getString("superInstanceId")).add(new InstanceRelationshipToChild(rel));
                        } else {
                          childInstanceMap.put(rel.getString("superInstanceId"), Arrays.asList(new InstanceRelationshipToChild(rel)));
                        }

                      }
                      log.info("Created relationships map" + parentInstanceMap);
                    } else {
                      log.info("No relations found for instance IDs ");
                    }
                    JsonResponse.success(routingContext.response(),
                            toRepresentation(it.getResult(),
                                    parentInstanceMap.get(it.getResult().getId()),
                                    childInstanceMap.get(it.getResult().getId()),
                                    context));
                  });
                }

              } else {
                ClientErrorResponse.notFound(routingContext.response());
              }
            }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private JsonObject toRepresentation(
          MultipleRecords<Instance> wrappedInstances,
          Map<String, List<InstanceRelationshipToParent>> parentMap,
          Map<String, List<InstanceRelationshipToChild>> childMap,
          WebContext context) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Instance> instances = wrappedInstances.records;

    instances.stream().forEach(instance -> {
      List<InstanceRelationshipToParent> parentInstances = parentMap.get(instance.getId());
      List<InstanceRelationshipToChild> childInstances = childMap.get(instance.getId());
      results.add(toRepresentation(instance, parentInstances, childInstances, context));
    });

    representation
      .put("instances", results)
      .put("totalRecords", wrappedInstances.totalRecords);

    return representation;
  }

  private JsonObject toRepresentation(Instance instance,
          List<InstanceRelationshipToParent> parentInstances,
          List<InstanceRelationshipToChild> childInstances,
          WebContext context) {
    JsonObject resp = new JsonObject();

    try {
      resp.put("@context", context.absoluteUrl(
        INSTANCES_PATH + "/context").toString());
    } catch (MalformedURLException e) {
      log.warn(
        String.format("Failed to create context link for instance: %s", e.toString()));
    }

    resp.put("id", instance.getId());
    resp.put(Instance.SOURCE_KEY, instance.getSource());
    resp.put(Instance.TITLE_KEY, instance.getTitle());
    putIfNotNull(resp, Instance.PARENT_INSTANCES_KEY, parentInstances);
    putIfNotNull(resp, Instance.CHILD_INSTANCES_KEY, childInstances);
    putIfNotNull(resp, Instance.ALTERNATIVE_TITLES_KEY, instance.getAlternativeTitles());
    putIfNotNull(resp, Instance.EDITION_KEY, instance.getEdition());
    putIfNotNull(resp, Instance.SERIES_KEY, instance.getSeries());
    putIfNotNull(resp, Instance.IDENTIFIERS_KEY, instance.getIdentifiers());
    putIfNotNull(resp, Instance.CONTRIBUTORS_KEY, instance.getContributors());
    putIfNotNull(resp, Instance.SUBJECTS_KEY, instance.getSubjects());
    putIfNotNull(resp, Instance.CLASSIFICATIONS_KEY, instance.getClassifications());
    putIfNotNull(resp, Instance.PUBLICATION_KEY, instance.getPublication());
    putIfNotNull(resp, Instance.URLS_KEY, instance.getUrls());
    putIfNotNull(resp, Instance.INSTANCE_TYPE_ID_KEY, instance.getInstanceTypeId());
    putIfNotNull(resp, Instance.INSTANCE_FORMAT_ID_KEY, instance.getInstanceFormatId());
    putIfNotNull(resp, Instance.PHYSICAL_DESCRIPTIONS_KEY, instance.getPhysicalDescriptions());
    putIfNotNull(resp, Instance.LANGUAGES_KEY, instance.getLanguages());
    putIfNotNull(resp, Instance.NOTES_KEY, instance.getNotes());
    putIfNotNull(resp, Instance.SOURCE_RECORD_FORMAT_KEY, instance.getSourceRecordFormat());
    putIfNotNull(resp, Instance.METADATA_KEY, instance.getMetadata());

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        INSTANCES_PATH, instance.getId()));

      resp.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      log.warn(
        String.format("Failed to create self link for instance: %s", e.toString()));
    }

    return resp;
  }

  private Instance requestToInstance(JsonObject instanceRequest) {
    List<Identifier> identifiers = instanceRequest.containsKey(Instance.IDENTIFIERS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.IDENTIFIERS_KEY)).stream()
          .map(json -> new Identifier(json))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<Contributor> contributors = instanceRequest.containsKey(Instance.CONTRIBUTORS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CONTRIBUTORS_KEY)).stream()
      .map(json -> new Contributor(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Classification> classifications = instanceRequest.containsKey(Instance.CLASSIFICATIONS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CLASSIFICATIONS_KEY)).stream()
      .map(json -> new Classification(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Publication> publications = instanceRequest.containsKey(Instance.PUBLICATION_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.PUBLICATION_KEY)).stream()
      .map(json -> new Publication(json))
      .collect(Collectors.toList())
      : new ArrayList<>();

    return new Instance(
      instanceRequest.getString("id"),
      instanceRequest.getString(Instance.SOURCE_KEY),
      instanceRequest.getString(Instance.TITLE_KEY),
      instanceRequest.getString(Instance.INSTANCE_TYPE_ID_KEY))
      .setAlternativeTitles(toListOfStrings(instanceRequest, Instance.ALTERNATIVE_TITLES_KEY))
      .setEdition(instanceRequest.getString(Instance.EDITION_KEY))
      .setSeries(toListOfStrings(instanceRequest, Instance.SERIES_KEY))
      .setIdentifiers(identifiers)
      .setContributors(contributors)
      .setSubjects(toListOfStrings(instanceRequest, Instance.SUBJECTS_KEY))
      .setClassifications(classifications)
      .setPublication(publications)
      .setUrls(toListOfStrings(instanceRequest, Instance.URLS_KEY))
      .setInstanceFormatId(instanceRequest.getString(Instance.INSTANCE_FORMAT_ID_KEY))
      .setPhysicalDescriptions(toListOfStrings(instanceRequest, Instance.PHYSICAL_DESCRIPTIONS_KEY))
      .setLanguages(toListOfStrings(instanceRequest, Instance.LANGUAGES_KEY))
      .setNotes(toListOfStrings(instanceRequest, Instance.NOTES_KEY))
      .setSourceRecordFormat(instanceRequest.getString(Instance.SOURCE_RECORD_FORMAT_KEY));
  }

  private void putIfNotNull(JsonObject target, String propertyName, String value) {
    if (value != null) {
      target.put(propertyName, value);
    }
  }

  private void putIfNotNull(JsonObject target, String propertyName, List<String> value) {
    if (value != null) {
      target.put(propertyName, value);
    }
  }

  private void putIfNotNull(JsonObject target, String propertyName, Object value) {
    if (value != null) {
      if (value instanceof List) {
        target.put(propertyName, value);
      } else {
        target.put(propertyName, new JsonObject(Json.encode(value)));
      }
    }
  }

  private List<String> toListOfStrings(JsonObject source, String propertyName) {
    return source.containsKey(propertyName)
      ? JsonArrayHelper.toListOfStrings(source.getJsonArray(propertyName))
      : new ArrayList<>();
  }

  private OkapiHttpClient createHttpClient(
          RoutingContext routingContext,
          WebContext context)
          throws MalformedURLException {

    return new OkapiHttpClient(routingContext.vertx().createHttpClient(),
            new URL(context.getOkapiLocation()), context.getTenantId(),
            context.getToken(),
            exception -> {
              ServerErrorResponse.internalError(routingContext.response(), String.format("Failed to contact storage module: %s",
                      exception.toString()));
            });
  }

}
