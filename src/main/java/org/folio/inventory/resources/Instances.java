package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.InstanceRelationship;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.RedirectResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.netty.util.internal.StringUtil.COMMA;
import static java.lang.String.format;
import static org.folio.inventory.support.http.server.SuccessResponse.noContent;

public class Instances extends AbstractInstances {
  private static final String INSTANCES_CONTEXT_PATH = INSTANCES_PATH + "/context";
  private static final String BLOCKED_FIELDS_CONFIG_PATH = INVENTORY_PATH + "/config/instances/blocked-fields";
  private static final String BLOCKED_FIELDS_UPDATE_ERROR_MESSAGE = "Instance is controlled by MARC record, "
    + "these fields are blocked and can not be updated: ";

  public Instances(final Storage storage, final HttpClient client) {
   super(storage, client);
  }

  public void register(Router router) {
    router.post(INSTANCES_PATH + "*").handler(BodyHandler.create());
    router.put(INSTANCES_PATH + "*").handler(BodyHandler.create());

    router.get(INSTANCES_CONTEXT_PATH).handler(this::getMetadataContext);
    router.get(BLOCKED_FIELDS_CONFIG_PATH).handler(this::getBlockedFieldsConfig);

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

  private void getBlockedFieldsConfig(RoutingContext routingContext) {
    JsonObject response = new JsonObject();
    response.put("blockedFields", new JsonArray(Json.encode(config.getInstanceBlockedFields())));
    JsonResponse.success(routingContext.response(), response);
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
          makeInstancesResponse(success, routingContext, context);
        },
        FailureResponseConsumer.serverError(routingContext.response())
      );
    } else {
      try {
        storage.getInstanceCollection(context).findByCql(
          search,
          pagingParameters,
          success -> {
            makeInstancesResponse(success, routingContext, context);
          },
          FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
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
        Instance response = success.getResult();
        response.setParentInstances(newInstance.getParentInstances());
        response.setChildInstances(newInstance.getChildInstances());
        updateInstanceRelationships(response, routingContext, context,
          x -> {
            try {
              URL url = context.absoluteUrl(format("%s/%s",
                INSTANCES_PATH, success.getResult().getId()));
              RedirectResponse.created(routingContext.response(), url.toString());
            } catch (MalformedURLException e) {
              log.warn(
                format("Failed to create self link for instance: %s", e.toString()));
            }
          }
        );
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void update(RoutingContext rContext) {
    WebContext wContext = new WebContext(rContext);
    JsonObject instanceRequest = rContext.getBodyAsJson();
    Instance updatedInstance = requestToInstance(instanceRequest);
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);

    instanceCollection.findById(rContext.request().getParam("id"), it -> {
        Instance existingInstance = it.getResult();
        if (existingInstance != null) {
          if (isInstanceControlledByRecord(existingInstance) && areInstanceBlockedFieldsChanged(existingInstance, updatedInstance)) {
            String errorMessage = BLOCKED_FIELDS_UPDATE_ERROR_MESSAGE + StringUtils.join(config.getInstanceBlockedFields(), COMMA);
            log.error(errorMessage);
            JsonResponse.unprocessableEntity(rContext.response(), errorMessage);
          } else {
            updateInstance(updatedInstance, rContext, wContext);
          }
        } else {
          ClientErrorResponse.notFound(rContext.response());
        }
      },
      FailureResponseConsumer.serverError(rContext.response()));
  }

  /**
   * Returns true if given Instance has linked record in source-record-storage
   *
   * @param instance given instance
   * @return boolean
   */
  private boolean isInstanceControlledByRecord(Instance instance) {
    return "MARC".equals(instance.getSource());
  }

  /**
   * Updates given Instance
   *
   * @param instance instance for update
   * @param rContext routing context
   * @param wContext web context
   */
  private void updateInstance(Instance instance, RoutingContext rContext, WebContext wContext) {
    InstanceCollection instanceCollection = storage.getInstanceCollection(wContext);
    instanceCollection.update(
      instance,
      v -> updateInstanceRelationships(instance, rContext, wContext, (x) -> noContent(rContext.response())),
      FailureResponseConsumer.serverError(rContext.response()));
  }

  /**
   * Compares existing instance with it's version for update,
   * returns true if blocked fields are changed
   *
   * @param existingInstance instance that exists in database
   * @param updatedInstance  instance with changes for update
   * @return boolean
   */
  private boolean areInstanceBlockedFieldsChanged(Instance existingInstance, Instance updatedInstance) {
    JsonObject existingInstanceJson = JsonObject.mapFrom(existingInstance);
    JsonObject updatedInstanceJson = JsonObject.mapFrom(updatedInstance);
    Map<String, Object> existingBlockedFields = new HashMap<>();
    Map<String, Object> updatedBlockedFields = new HashMap<>();
    for (String blockedFieldCode : config.getInstanceBlockedFields()) {
      existingBlockedFields.put(blockedFieldCode, existingInstanceJson.getValue(blockedFieldCode));
      updatedBlockedFields.put(blockedFieldCode, updatedInstanceJson.getValue(blockedFieldCode));
    }
    return ObjectUtils.notEqual(existingBlockedFields, updatedBlockedFields);
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).empty(
      v -> noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).delete(
      routingContext.request().getParam("id"),
      v -> noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void getById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getInstanceCollection(context).findById(
      routingContext.request().getParam("id"),
      it -> {
        if (it.getResult() != null) {
          makeInstanceResponse(it, routingContext, context);
        } else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }



  /**
   * Fetches instance relationships for multiple Instance records, populates, responds
   *
   * @param success        Multi record Instances result
   * @param routingContext
   * @param context
   */
  private void makeInstancesResponse(
    Success<MultipleRecords<Instance>> success,
    RoutingContext routingContext,
    WebContext context) {

    List<String> instanceIds = getInstanceIdsFromInstanceResult(success);
    String query = createQueryForRelatedInstances(instanceIds);

    try {
      query = URLEncoder.encode(query, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error(format("Cannot encode query %s", query));
    }
    CollectionResourceClient relatedInstancesClient = createInstanceRelationshipsClient(routingContext, context);

    if (relatedInstancesClient != null) {
      relatedInstancesClient.getMany(query, (Response result) -> {
        Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap();
        Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap();
        if (result.getStatusCode() == 200) {
          JsonObject json = result.getJson();
          List<JsonObject> relationsList = JsonArrayHelper.toList(json.getJsonArray("instanceRelationships"));
          relationsList.stream().map(rel -> {
            addToList(childInstanceMap, rel.getString("superInstanceId"), new InstanceRelationshipToChild(rel));
            return rel;
          }).forEachOrdered(rel -> {
            addToList(parentInstanceMap, rel.getString("subInstanceId"), new InstanceRelationshipToParent(rel));
          });
        }
        JsonResponse.success(routingContext.response(),
          toRepresentation(success.getResult(), parentInstanceMap, childInstanceMap, context));
      });
    }
  }

  /**
   * Fetches instance relationships for a single Instance result, populates, responds
   *
   * @param success        Single record Instance result
   * @param routingContext
   * @param context
   */
  private void makeInstanceResponse(
    Success<Instance> success,
    RoutingContext routingContext,
    WebContext context) {

    Instance instance = success.getResult();
    List<String> instanceIds = getInstanceIdsFromInstanceResult(success);
    String query = createQueryForRelatedInstances(instanceIds);
    CollectionResourceClient relatedInstancesClient = createInstanceRelationshipsClient(routingContext, context);

    if (relatedInstancesClient != null) {
      relatedInstancesClient.getMany(query, (Response result) -> {
        List<InstanceRelationshipToParent> parentInstanceList = new ArrayList();
        List<InstanceRelationshipToChild> childInstanceList = new ArrayList();
        if (result.getStatusCode() == 200) {
          JsonObject json = result.getJson();
          List<JsonObject> relationsList = JsonArrayHelper.toList(json.getJsonArray("instanceRelationships"));
          relationsList.forEach(rel -> {
            if (rel.getString(InstanceRelationship.SUPER_INSTANCE_ID_KEY).equals(instance.getId())) {
              childInstanceList.add(new InstanceRelationshipToChild(rel));
            } else if (rel.getString(InstanceRelationship.SUB_INSTANCE_ID_KEY).equals(instance.getId())) {
              parentInstanceList.add(new InstanceRelationshipToParent(rel));
            }
          });
        }
        JsonResponse.success(
          routingContext.response(),
          toRepresentation(
            success.getResult(),
            parentInstanceList,
            childInstanceList,
            context));
      });
    }
  }

  // Utilities

  private List<String> getInstanceIdsFromInstanceResult(Success success) {
    List<String> instanceIds = new ArrayList();
    if (success.getResult() instanceof Instance) {
      instanceIds = Arrays.asList(((Instance) success.getResult()).getId());
    } else if (success.getResult() instanceof MultipleRecords) {
      instanceIds = (((MultipleRecords<Instance>) success.getResult()).records.stream()
        .map(instance -> instance.getId())
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList()));
    }
    return instanceIds;
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

}
