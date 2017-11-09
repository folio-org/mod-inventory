package org.folio.inventory.resources;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventory.CollectionResourceClient;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.Item;
import org.folio.inventory.domain.ItemCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventory.common.FutureAssistance.allOf;

public class Items {
  private final Storage storage;

  public Items(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(relativeItemsPath() + "*").handler(BodyHandler.create());
    router.put(relativeItemsPath() + "*").handler(BodyHandler.create());

    router.get(relativeItemsPath()).handler(this::getAll);
    router.post(relativeItemsPath()).handler(this::create);
    router.delete(relativeItemsPath()).handler(this::deleteAll);

    router.get(relativeItemsPath() + "/:id").handler(this::getById);
    router.put(relativeItemsPath() + "/:id").handler(this::update);
    router.delete(relativeItemsPath() + "/:id").handler(this::deleteById);
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

  void deleteAll(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    storage.getItemCollection(context).empty(
      v -> SuccessResponse.noContent(routingContext.response()),
      FailureResponseConsumer.serverError(routingContext.response()));
  }

  void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject item = routingContext.getBodyAsJson();

    Item newItem = requestToItem(item);

    ItemCollection itemCollection = storage.getItemCollection(context);

    if(newItem.barcode != null) {
      try {
        itemCollection.findByCql(String.format("barcode=%s", newItem.barcode),
          PagingParameters.defaults(), findResult -> {

            if(findResult.getResult().records.size() == 0) {
              itemCollection.add(newItem, success -> {
                try {
                  URL url = context.absoluteUrl(String.format("%s/%s",
                    relativeItemsPath(), success.getResult().id));

                  RedirectResponse.created(routingContext.response(), url.toString());
                } catch (MalformedURLException e) {
                  System.out.println(
                    String.format("Failed to create self link for item: " + e.toString()));
                }
              }, FailureResponseConsumer.serverError(routingContext.response()));
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
      itemCollection.add(newItem, success -> {
        try {
          URL url = context.absoluteUrl(String.format("%s/%s",
            relativeItemsPath(), success.getResult().id));

          RedirectResponse.created(routingContext.response(), url.toString());
        } catch (MalformedURLException e) {
          System.out.println(
            String.format("Failed to create self link for item: " + e.toString()));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
    }
  }

  private void update(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject itemRequest = routingContext.getBodyAsJson();

    Item updatedItem = requestToItem(itemRequest);

    ItemCollection itemCollection = storage.getItemCollection(context);

    itemCollection.findById(routingContext.request().getParam("id"), getItemResult -> {
      if(getItemResult.getResult() != null) {
        if(updatedItem.barcode == null || getItemResult.getResult().barcode == updatedItem.barcode) {
          itemCollection.update(updatedItem,
            v -> SuccessResponse.noContent(routingContext.response()),
            failure -> ServerErrorResponse.internalError(
              routingContext.response(), failure.getReason()));
        } else {
          try {
            itemCollection.findByCql(
              String.format("barcode=%s and id<>%s", updatedItem.barcode, updatedItem.id),
              PagingParameters.defaults(), it -> {

              List<Item> items = it.getResult().records;

              if(items.size() == 0) {
                itemCollection.update(updatedItem,
                  v -> SuccessResponse.noContent(routingContext.response()),
                  failure -> ServerErrorResponse.internalError(
                    routingContext.response(), failure.getReason()));
              }
              else {
                ClientErrorResponse.badRequest(routingContext.response(),
                  String.format("Barcode must be unique, %s is already assigned to another item",
                    updatedItem.barcode));
              }
            }, FailureResponseConsumer.serverError(routingContext.response()));
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
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

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
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      materialTypesClient = createMaterialTypesClient(client, context);
      loanTypesClient = createLoanTypesClient(client, context);
      locationsClient = createLocationsClient(client, context);
    }
    catch (MalformedURLException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

      return;
    }

    storage.getItemCollection(context).findById(
      routingContext.request().getParam("id"),
      (Success<Item> itemResponse) -> {
        Item item = itemResponse.getResult();

        if(item != null) {
          CompletableFuture<Response> materialTypeFuture = new CompletableFuture<>();
          CompletableFuture<Response> permanentLoanTypeFuture = new CompletableFuture<>();
          CompletableFuture<Response> temporaryLoanTypeFuture = new CompletableFuture<>();
          CompletableFuture<Response> temporaryLocationFuture = new CompletableFuture<>();
          CompletableFuture<Response> permanentLocationFuture = new CompletableFuture<>();

          ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

          if(item.materialTypeId != null) {
            allFutures.add(materialTypeFuture);

            materialTypesClient.get(item.materialTypeId,
              materialTypeFuture::complete);
          }

          if(item.permanentLoanTypeId != null) {
            allFutures.add(permanentLoanTypeFuture);

            loanTypesClient.get(item.permanentLoanTypeId,
              permanentLoanTypeFuture::complete);
          }

          if(item.temporaryLoanTypeId != null) {
            allFutures.add(temporaryLoanTypeFuture);

            loanTypesClient.get(item.temporaryLoanTypeId,
              temporaryLoanTypeFuture::complete);
          }

          if(item.permanentLocationId != null) {
						allFutures.add(permanentLocationFuture);

						locationsClient.get(item.permanentLocationId,
              permanentLocationFuture::complete);
          }

          if(item.temporaryLocationId != null){
            allFutures.add(temporaryLocationFuture);
            locationsClient.get(item.temporaryLocationId,
              temporaryLocationFuture::complete);
          }

          CompletableFuture<Void> allDoneFuture = allOf(allFutures);

          allDoneFuture.thenAccept(v -> {
            JsonObject foundMaterialType =
              referenceRecordFrom(item.materialTypeId, materialTypeFuture);

            JsonObject foundPermanentLoanType =
              referenceRecordFrom(item.permanentLoanTypeId, permanentLoanTypeFuture);

            JsonObject foundTemporaryLoanType =
              referenceRecordFrom(item.temporaryLoanTypeId, temporaryLoanTypeFuture);

            JsonObject foundPermanentLocation =
              referenceRecordFrom(item.permanentLocationId, permanentLocationFuture);

            JsonObject foundTemporaryLocation =
              referenceRecordFrom(item.temporaryLocationId, temporaryLocationFuture);

            try {
              JsonObject representation = new ItemRepresentation(relativeItemsPath())
                .toJson(item,
                foundMaterialType,
                foundPermanentLoanType,
                foundTemporaryLoanType,
                foundPermanentLocation,
                foundTemporaryLocation,
                context);

              JsonResponse.success(routingContext.response(), representation);

            } catch(Exception e) {
              ServerErrorResponse.internalError(routingContext.response(),
                "Error creating Item Representation: " + e.getLocalizedMessage());
            }
          });
        }
        else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private static String relativeItemsPath() {
    return "/inventory/items";
  }

  private Item requestToItem(JsonObject itemRequest) {
    String status = getNestedProperty(itemRequest, "status", "name");
    String materialTypeId = getNestedProperty(itemRequest, "materialType", "id");
    String permanentLocationId = getNestedProperty(itemRequest, "permanentLocation", "id");
    String temporaryLocationId = getNestedProperty(itemRequest, "temporaryLocation", "id");
    String permanentLoanTypeId = getNestedProperty(itemRequest, "permanentLoanType", "id");
    String temporaryLoanTypeId = getNestedProperty(itemRequest, "temporaryLoanType", "id");

    return new Item(
      itemRequest.getString("id"),
      itemRequest.getString("title"),
      itemRequest.getString("barcode"),
      itemRequest.getString("instanceId"),
      status,
      materialTypeId,
      permanentLocationId,
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

    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      materialTypesClient = createMaterialTypesClient(client, context);
      loanTypesClient = createLoanTypesClient(client, context);
      locationsClient = createLocationsClient(client, context);
    }
    catch (MalformedURLException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

      return;
    }

    ArrayList<CompletableFuture<Response>> allMaterialTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLoanTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLocationsFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

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
      .map(item -> item.permanentLocationId)
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
      System.out.println("GET all items: all futures completed");

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
          new ItemRepresentation(relativeItemsPath())
            .toJson(wrappedItems, foundMaterialTypes, foundLoanTypes, foundLocations, context));
      }
      catch(Exception e) {
        ServerErrorResponse.internalError(routingContext.response(), e.toString());
      }
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

    return id != null &&
      requestFuture.join().getStatusCode() == 200 ?
      requestFuture.join().getJson() : null;
  }
}
