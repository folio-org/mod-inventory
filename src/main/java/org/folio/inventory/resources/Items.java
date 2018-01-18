package org.folio.inventory.resources;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.Item;
import org.folio.inventory.domain.ItemCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.HoldingsSupport;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventory.common.FutureAssistance.allOf;
import static org.folio.inventory.support.CqlHelper.multipleRecordsCqlQuery;

public class Items {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String RELATIVE_ITEMS_PATH = "/inventory/items";

  private final Storage storage;

  public Items(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(RELATIVE_ITEMS_PATH + "*").handler(BodyHandler.create());
    router.put(RELATIVE_ITEMS_PATH + "*").handler(BodyHandler.create());

    router.get(RELATIVE_ITEMS_PATH).handler(this::getAll);
    router.post(RELATIVE_ITEMS_PATH).handler(this::create);
    router.delete(RELATIVE_ITEMS_PATH).handler(this::deleteAll);

    router.get(RELATIVE_ITEMS_PATH + "/:id").handler(this::getById);
    router.put(RELATIVE_ITEMS_PATH + "/:id").handler(this::update);
    router.delete(RELATIVE_ITEMS_PATH + "/:id").handler(this::deleteById);
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
      storage.getItemCollection(context).findAll(
        pagingParameters,
        success -> respondWithManyItems(routingContext, context, success.getResult()),
        FailureResponseConsumer.serverError(routingContext.response()));
    }
    else {
      try {
        storage.getItemCollection(context).findByCql(search,
          pagingParameters, success ->
            respondWithManyItems(routingContext, context, success.getResult()),
          FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
    }
  }

  private void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getItemCollection(context).empty(
      v -> SuccessResponse.noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject item = routingContext.getBodyAsJson();

    Item newItem = requestToItem(item);

    ItemCollection itemCollection = storage.getItemCollection(context);

    if(newItem.barcode != null) {
      try {
        itemCollection.findByCql(CqlHelper.barcodeIs(newItem.barcode),
          PagingParameters.defaults(), findResult -> {

            if(findResult.getResult().records.isEmpty()) {
              addItem(routingContext, context, newItem, itemCollection);
            }
            else {
              ClientErrorResponse.badRequest(routingContext.response(),
                String.format("Barcode must be unique, %s is already assigned to another item",
                  newItem.barcode));
            }
          }, FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
    }
    else {
      addItem(routingContext, context, newItem, itemCollection);
    }
  }

  private void update(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject itemRequest = routingContext.getBodyAsJson();

    Item updatedItem = requestToItem(itemRequest);

    ItemCollection itemCollection = storage.getItemCollection(context);

    itemCollection.findById(routingContext.request().getParam("id"), getItemResult -> {
      if(getItemResult.getResult() != null) {
        if(hasSameBarcode(updatedItem, getItemResult.getResult())) {
          updateItem(routingContext, updatedItem, itemCollection);
        } else {
          try {
            checkForNonUniqueBarcode(routingContext, updatedItem, itemCollection);
          } catch (UnsupportedEncodingException e) {
            ServerErrorResponse.internalError(routingContext.response(), e.toString());
          }
        }
      }
      else {
        ClientErrorResponse.notFound(routingContext.response());
      }
    }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void deleteById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    CollectionResourceClient itemsStorageClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      itemsStorageClient = createItemsStorageClient(client, context);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, context);

      return;
    }

    String id = routingContext.request().getParam("id");

    itemsStorageClient.delete(id, response -> {
      if(response.getStatusCode() == 204) {
        SuccessResponse.noContent(routingContext.response());
      }
      else {
        ForwardResponse.forward(routingContext.response(), response);
      }
    });
  }

  private void getById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    CollectionResourceClient holdingsClient;
    CollectionResourceClient instancesClient;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      holdingsClient = createHoldingsClient(client, context);
      instancesClient = createInstancesClient(client, context);
      materialTypesClient = createMaterialTypesClient(client, context);
      loanTypesClient = createLoanTypesClient(client, context);
      locationsClient = createLocationsClient(client, context);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, context);

      return;
    }

    storage.getItemCollection(context).findById(
      routingContext.request().getParam("id"),
      (Success<Item> itemResponse) -> {
        Item item = itemResponse.getResult();

        if(item != null) {
          holdingsClient.get(item.holdingId, holdingResponse -> {
            String instanceId = holdingResponse.getStatusCode() == 200
              ? holdingResponse.getJson().getString("instanceId")
              : null;

            instancesClient.get(instanceId, instanceResponse -> {
              final JsonObject instance = instanceResponse.getStatusCode() == 200
                ? instanceResponse.getJson()
                : null;

              String permanentLocationId = holdingResponse.getStatusCode() == 200
                && holdingResponse.getJson().containsKey("permanentLocationId")
                ? holdingResponse.getJson().getString("permanentLocationId")
                : null;

              ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

              CompletableFuture<Response> materialTypeFuture = getReferenceRecord(
                item.materialTypeId, materialTypesClient, allFutures);

              CompletableFuture<Response> permanentLoanTypeFuture = getReferenceRecord(
                item.permanentLoanTypeId, loanTypesClient, allFutures);

              CompletableFuture<Response> temporaryLoanTypeFuture = getReferenceRecord(
                item.temporaryLoanTypeId, loanTypesClient, allFutures);

              CompletableFuture<Response> permanentLocationFuture = getReferenceRecord(
                permanentLocationId, locationsClient, allFutures);

              CompletableFuture<Response> temporaryLocationFuture = getReferenceRecord(
                item.temporaryLocationId, locationsClient, allFutures);

              CompletableFuture<Void> allDoneFuture = allOf(allFutures);

              allDoneFuture.thenAccept(v -> {
                try {
                  JsonObject representation = includeReferenceRecordInformationInItem(
                    context, item, instance, materialTypeFuture, permanentLocationId,
                    permanentLoanTypeFuture, temporaryLoanTypeFuture,
                    temporaryLocationFuture, permanentLocationFuture);

                  JsonResponse.success(routingContext.response(), representation);
                } catch (Exception e) {
                  ServerErrorResponse.internalError(routingContext.response(),
                    String.format("Error creating Item Representation: %s", e));
                }
              });
              });

          });
        }
        else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private Item requestToItem(JsonObject itemRequest) {
    List<String> pieceIdentifiers;
    pieceIdentifiers = JsonArrayHelper.toListOfStrings(itemRequest.getJsonArray("pieceIdentifiers"));
    String status = getNestedProperty(itemRequest, "status", "name");
    List<String> notes;
    notes = JsonArrayHelper.toListOfStrings(itemRequest.getJsonArray("notes"));
    String materialTypeId = getNestedProperty(itemRequest, "materialType", "id");
    String temporaryLocationId = getNestedProperty(itemRequest, "temporaryLocation", "id");
    String permanentLoanTypeId = getNestedProperty(itemRequest, "permanentLoanType", "id");
    String temporaryLoanTypeId = getNestedProperty(itemRequest, "temporaryLoanType", "id");

    return new Item(
      itemRequest.getString("id"),
      itemRequest.getString("barcode"),
      itemRequest.getString("enumeration"),
      itemRequest.getString("chronology"),
      pieceIdentifiers,
      itemRequest.getString("numberOfPieces"),
      itemRequest.getString("holdingsRecordId"),
      notes,
      status,
      materialTypeId,
      temporaryLocationId,
      permanentLoanTypeId,
      temporaryLoanTypeId);
  }

  private String getNestedProperty(
    JsonObject itemRequest,
    String objectPropertyName, String nestedPropertyName) {

    return itemRequest.containsKey(objectPropertyName)
      ? itemRequest.getJsonObject(objectPropertyName).getString(nestedPropertyName)
      : null;
  }

  private void respondWithManyItems(
    RoutingContext routingContext,
    WebContext context,
    MultipleRecords<Item> wrappedItems) {

    CollectionResourceClient holdingsClient;
    CollectionResourceClient instancesClient;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      holdingsClient = createHoldingsClient(client, context);
      instancesClient = createInstancesClient(client, context);
      materialTypesClient = createMaterialTypesClient(client, context);
      loanTypesClient = createLoanTypesClient(client, context);
      locationsClient = createLocationsClient(client, context);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, context);

      return;
    }

    ArrayList<CompletableFuture<Response>> allMaterialTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLoanTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLocationsFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

    List<String> holdingsIds = wrappedItems.records.stream()
      .map(item -> item.holdingId)
      .filter(Objects::nonNull)
      .distinct()
      .collect(Collectors.toList());

    CompletableFuture<Response> holdingsFetched =
      new CompletableFuture<>();

    String holdingsQuery = multipleRecordsCqlQuery(holdingsIds);

    holdingsClient.getMany(holdingsQuery, holdingsIds.size(), 0,
      holdingsFetched::complete);

    holdingsFetched.thenAccept(holdingsResponse -> {
      if (holdingsResponse.getStatusCode() != 200) {
        ServerErrorResponse.internalError(routingContext.response(),
          String.format("Holdings request (%s) failed %s: %s",
            holdingsQuery, holdingsResponse.getStatusCode(),
            holdingsResponse.getBody()));
      }

      final List<JsonObject> holdings = JsonArrayHelper.toList(
        holdingsResponse.getJson().getJsonArray("holdingsRecords"));

      List<String> instanceIds = holdings.stream()
        .map(holding -> holding.getString("instanceId"))
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList());

      CompletableFuture<Response> instancesFetched =
        new CompletableFuture<>();

      String instancesQuery = multipleRecordsCqlQuery(instanceIds);

      instancesClient.getMany(instancesQuery, instanceIds.size(), 0,
        instancesFetched::complete);

      instancesFetched.thenAccept(instancesResponse -> {
        if (instancesResponse.getStatusCode() != 200) {
          ServerErrorResponse.internalError(routingContext.response(),
            String.format("Instances request (%s) failed %s: %s",
              instancesQuery, instancesResponse.getStatusCode(),
              instancesResponse.getBody()));
        }

        final List<JsonObject> instances = JsonArrayHelper.toList(
          instancesResponse.getJson().getJsonArray("instances"));

        List<String> materialTypeIds = wrappedItems.records.stream()
          .map(item -> item.materialTypeId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        materialTypeIds.stream().forEach(id -> {
          CompletableFuture<Response> newFuture = new CompletableFuture<>();

          allFutures.add(newFuture);
          allMaterialTypeFutures.add(newFuture);

          materialTypesClient.get(id, newFuture::complete);
        });

        List<String> permanentLoanTypeIds = wrappedItems.records.stream()
          .map(item -> item.permanentLoanTypeId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLoanTypeIds = wrappedItems.records.stream()
          .map(item -> item.temporaryLoanTypeId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        Stream.concat(permanentLoanTypeIds.stream(), temporaryLoanTypeIds.stream())
          .distinct()
          .forEach(id -> {

            CompletableFuture<Response> newFuture = new CompletableFuture<>();

            allFutures.add(newFuture);
            allLoanTypeFutures.add(newFuture);

            loanTypesClient.get(id, newFuture::complete);
          });

        List<String> permanentLocationIds = wrappedItems.records.stream()
          .map(item -> HoldingsSupport.determinePermanentLocationIdForItem(
            HoldingsSupport.holdingForItem(item, holdings).orElse(null)))
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLocationIds = wrappedItems.records.stream()
          .map(item -> item.temporaryLocationId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        Stream.concat(permanentLocationIds.stream(), temporaryLocationIds.stream())
          .distinct()
          .forEach(id -> {

            CompletableFuture<Response> newFuture = new CompletableFuture<>();

            allFutures.add(newFuture);
            allLocationsFutures.add(newFuture);

            locationsClient.get(id, newFuture::complete);
          });

        CompletableFuture<Void> allDoneFuture = allOf(allFutures);

        allDoneFuture.thenAccept(v -> {
          log.debug("GET all items: all futures completed");

          try {
            Map<String, JsonObject> foundMaterialTypes
              = allMaterialTypeFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

            Map<String, JsonObject> foundLoanTypes
              = allLoanTypeFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

            Map<String, JsonObject> foundLocations
              = allLocationsFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

            JsonResponse.success(routingContext.response(),
              new ItemRepresentation(RELATIVE_ITEMS_PATH)
                .toJson(wrappedItems, holdings, instances, foundMaterialTypes,
                  foundLoanTypes, foundLocations, context));
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(), e.toString());
          }
        });
      });
    });
  }

  private OkapiHttpClient createHttpClient(
    RoutingContext routingContext,
    WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient(routingContext.vertx().createHttpClient(),
      new URL(context.getOkapiLocation()), context.getTenantId(),
      context.getToken(),
      exception -> ServerErrorResponse.internalError(routingContext.response(),
      String.format("Failed to contact storage module: %s",
        exception.toString())));
  }

  private CollectionResourceClient createItemsStorageClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/item-storage/items");
  }

  private CollectionResourceClient createHoldingsClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/holdings-storage/holdings");
  }

  private CollectionResourceClient createInstancesClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/instance-storage/instances");
  }

  private CollectionResourceClient createMaterialTypesClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/material-types");
  }

  private CollectionResourceClient createLoanTypesClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/loan-types");
  }

  private CollectionResourceClient createLocationsClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context,
      "/shelf-locations");
  }

  private CollectionResourceClient createCollectionResourceClient(
    OkapiHttpClient client,
    WebContext context,
    String rootPath)
    throws MalformedURLException {

    return new CollectionResourceClient(client,
      new URL(context.getOkapiLocation() + rootPath));
  }

  private JsonObject referenceRecordFrom(
    String id,
    CompletableFuture<Response> requestFuture) {

    return id != null
      && requestFuture != null
      && requestFuture.join() != null
      && requestFuture.join().getStatusCode() == 200
      ? requestFuture.join().getJson()
      : null;
  }

  private void addItem(
    RoutingContext routingContext,
    WebContext context,
    Item newItem,
    ItemCollection itemCollection) {

    itemCollection.add(newItem, success -> {
      try {
        URL url = context.absoluteUrl(String.format("%s/%s",
          RELATIVE_ITEMS_PATH, success.getResult().id));

        RedirectResponse.created(routingContext.response(), url.toString());
      } catch (MalformedURLException e) {
        log.warn(String.format("Failed to create self link for item: %s", e.toString()));
      }
    }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void invalidOkapiUrlResponse(RoutingContext routingContext, WebContext context) {
    ServerErrorResponse.internalError(routingContext.response(),
      String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));
  }

  private CompletableFuture<Response> getReferenceRecord(
    String id, CollectionResourceClient client,
    ArrayList<CompletableFuture<Response>> allFutures) {

    CompletableFuture<Response> newFuture = new CompletableFuture<>();

    if(id != null) {
      allFutures.add(newFuture);

      client.get(id, newFuture::complete);

      return newFuture;
    }
    else {
      return null;
    }
  }

  private JsonObject includeReferenceRecordInformationInItem(
    WebContext context,
    Item item,
    JsonObject instance,
    CompletableFuture<Response> materialTypeFuture,
    String permanentLocationId,
    CompletableFuture<Response> permanentLoanTypeFuture,
    CompletableFuture<Response> temporaryLoanTypeFuture,
    CompletableFuture<Response> temporaryLocationFuture,
    CompletableFuture<Response> permanentLocationFuture) {

    JsonObject foundMaterialType =
      referenceRecordFrom(item.materialTypeId, materialTypeFuture);

    JsonObject foundPermanentLoanType =
      referenceRecordFrom(item.permanentLoanTypeId, permanentLoanTypeFuture);

    JsonObject foundTemporaryLoanType =
      referenceRecordFrom(item.temporaryLoanTypeId, temporaryLoanTypeFuture);

    JsonObject foundPermanentLocation =
      referenceRecordFrom(permanentLocationId, permanentLocationFuture);

    JsonObject foundTemporaryLocation =
      referenceRecordFrom(item.temporaryLocationId, temporaryLocationFuture);

    return new ItemRepresentation(RELATIVE_ITEMS_PATH)
        .toJson(item,
          instance,
          foundMaterialType,
          foundPermanentLoanType,
          foundTemporaryLoanType,
          foundPermanentLocation,
          foundTemporaryLocation,
          context);
  }

  private boolean hasSameBarcode(Item updatedItem, Item foundItem) {
    return updatedItem.barcode == null
      || Objects.equals(foundItem.barcode, updatedItem.barcode);
  }

  private void updateItem(
    RoutingContext routingContext,
    Item updatedItem,
    ItemCollection itemCollection) {

    itemCollection.update(updatedItem,
      v -> SuccessResponse.noContent(routingContext.response()),
      failure -> ServerErrorResponse.internalError(
        routingContext.response(), failure.getReason()));
  }

  private void checkForNonUniqueBarcode(
    RoutingContext routingContext,
    Item updatedItem,
    ItemCollection itemCollection)
    throws UnsupportedEncodingException {

    itemCollection.findByCql(
      CqlHelper.barcodeIs(updatedItem.barcode) + " and id<>" + updatedItem.id,
      PagingParameters.defaults(), it -> {

        List<Item> items = it.getResult().records;

        if(items.isEmpty()) {
          updateItem(routingContext, updatedItem, itemCollection);
        }
        else {
          ClientErrorResponse.badRequest(routingContext.response(),
            String.format("Barcode must be unique, %s is already assigned to another item",
              updatedItem.barcode));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

}


