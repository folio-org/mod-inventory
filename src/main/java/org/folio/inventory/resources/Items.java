package org.folio.inventory.resources;

import static org.folio.inventory.common.FutureAssistance.allOf;
import static org.folio.inventory.support.CqlHelper.multipleRecordsCqlQuery;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.HoldingsSupport;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

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

    if(newItem.getBarcode() != null) {
      try {
        itemCollection.findByCql(CqlHelper.barcodeIs(newItem.getBarcode()),
          PagingParameters.defaults(), findResult -> {

            if(findResult.getResult().records.isEmpty()) {
              addItem(routingContext, context, newItem, itemCollection);
            }
            else {
              ClientErrorResponse.badRequest(routingContext.response(),
                String.format("Barcode must be unique, %s is already assigned to another item",
                  newItem.getBarcode()));
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
          holdingsClient.get(item.getHoldingId(), holdingResponse -> {
            final JsonObject holding = holdingResponse.getStatusCode() == 200
              ? holdingResponse.getJson()
              : null;

            String instanceId = holdingResponse.getStatusCode() == 200
              ? holdingResponse.getJson().getString("instanceId")
              : null;

            instancesClient.get(instanceId, instanceResponse -> {
              final JsonObject instance = instanceResponse.getStatusCode() == 200
                ? instanceResponse.getJson()
                : null;

              String effectiveLocationId = holdingResponse.getStatusCode() == 200
                ? HoldingsSupport.determineEffectiveLocationIdForItem(
                    holdingResponse.getJson(), item)
                : null;

              log.info("Effective location ID in Items: " + effectiveLocationId);

              ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

              CompletableFuture<Response> materialTypeFuture = getReferenceRecord(
                item.getMaterialTypeId(), materialTypesClient, allFutures);

              CompletableFuture<Response> permanentLoanTypeFuture = getReferenceRecord(
                item.getPermanentLoanTypeId(), loanTypesClient, allFutures);

              CompletableFuture<Response> temporaryLoanTypeFuture = getReferenceRecord(
                item.getTemporaryLoanTypeId(), loanTypesClient, allFutures);

              CompletableFuture<Response> permanentLocationFuture = getReferenceRecord(
                item.getPermanentLocationId(), locationsClient, allFutures);

              CompletableFuture<Response> temporaryLocationFuture = getReferenceRecord(
                item.getTemporaryLocationId(), locationsClient, allFutures);

              CompletableFuture<Response> effectiveLocationFuture = getReferenceRecord(
                effectiveLocationId, locationsClient, allFutures);

              CompletableFuture<Void> allDoneFuture = allOf(allFutures);

              allDoneFuture.thenAccept(v -> {
                try {
                  JsonObject representation = includeReferenceRecordInformationInItem(
                    context, item, holding, instance, materialTypeFuture,
                    effectiveLocationId,
                    permanentLoanTypeFuture, temporaryLoanTypeFuture,
                    temporaryLocationFuture, permanentLocationFuture,
                    effectiveLocationFuture);

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
    List<String> formerIds = toListOfStrings(
      itemRequest.getJsonArray(Item.FORMER_IDS_KEY));

    List<String> copyNumbers = toListOfStrings(
      itemRequest.getJsonArray("copyNumbers"));

    List<String> statisticalCodeIds = toListOfStrings(
      itemRequest.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));

    List<String> yearCaption = toListOfStrings(
      itemRequest.getJsonArray(Item.YEAR_CAPTION_KEY));

    String status = getNestedProperty(itemRequest, "status", "name");

    List<Note> notes = itemRequest.containsKey(Item.NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.NOTES_KEY)).stream()
          .map(json -> new Note(json))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<ElectronicAccess> electronicAccess = itemRequest.containsKey(Item.ELECTRONIC_ACCESS_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.ELECTRONIC_ACCESS_KEY)).stream()
          .map(json -> new ElectronicAccess(json))
          .collect(Collectors.toList())
          : new ArrayList<>();


    String materialTypeId = getNestedProperty(itemRequest, "materialType", "id");
    String permanentLocationId = getNestedProperty(itemRequest, "permanentLocation", "id");
    String temporaryLocationId = getNestedProperty(itemRequest, "temporaryLocation", "id");
    String permanentLoanTypeId = getNestedProperty(itemRequest, "permanentLoanType", "id");
    String temporaryLoanTypeId = getNestedProperty(itemRequest, "temporaryLoanType", "id");

    return new Item(
      itemRequest.getString("id"),
      itemRequest.getString("holdingsRecordId"),
      status,
      materialTypeId,
      permanentLoanTypeId,
      null)
            .setHrid(itemRequest.getString(Item.HRID_KEY))
            .setFormerIds(formerIds)
            .setDiscoverySuppress(itemRequest.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
            .setBarcode(itemRequest.getString("barcode"))
            .setItemLevelCallNumber(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
            .setItemLevelCallNumberPrefix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
            .setItemLevelCallNumberSuffix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
            .setItemLevelCallNumberTypeId(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
            .setVolume(itemRequest.getString(Item.VOLUME_KEY))
            .setEnumeration(itemRequest.getString("enumeration"))
            .setChronology(itemRequest.getString("chronology"))
            .setNumberOfPieces(itemRequest.getString("numberOfPieces"))
            .setDescriptionOfPieces(itemRequest.getString(Item.DESCRIPTION_OF_PIECES_KEY))
            .setNumberOfMissingPieces(itemRequest.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
            .setMissingPieces(itemRequest.getString(Item.MISSING_PIECES_KEY))
            .setMissingPiecesDate(itemRequest.getString(Item.MISSING_PIECES_DATE_KEY))
            .setItemDamagedStatusId(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
            .setItemDamagedStatusDate(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
            .setPermanentLocationId(permanentLocationId)
            .setTemporaryLocationId(temporaryLocationId)
            .setTemporaryLoanTypeId(temporaryLoanTypeId)
            .setCopyNumbers(copyNumbers)
            .setNotes(notes)
            .setAccessionNumber(itemRequest.getString(Item.ACCESSION_NUMBER_KEY))
            .setItemIdentifier(itemRequest.getString(Item.ITEM_IDENTIFIER_KEY))
            .setYearCaption(yearCaption)
            .setElectronicAccess(electronicAccess)
            .setStatisticalCodeIds(statisticalCodeIds);
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
    CollectionResourceClient effectiveLocationsClient;

    try {
      OkapiHttpClient client = createHttpClient(routingContext, context);
      holdingsClient = createHoldingsClient(client, context);
      instancesClient = createInstancesClient(client, context);
      materialTypesClient = createMaterialTypesClient(client, context);
      loanTypesClient = createLoanTypesClient(client, context);
      locationsClient = createLocationsClient(client, context);
      effectiveLocationsClient = createLocationsClient(client, context);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, context);

      return;
    }

    ArrayList<CompletableFuture<Response>> allMaterialTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLoanTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allEffectiveLocationsFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLocationsFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

    List<String> holdingsIds = wrappedItems.records.stream()
      .map(item -> item.getHoldingId())
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
          .map(item -> item.getMaterialTypeId())
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
          .map(item -> item.getPermanentLoanTypeId())
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLoanTypeIds = wrappedItems.records.stream()
          .map(item -> item.getTemporaryLoanTypeId())
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

        List<String> effectiveLocationIds = wrappedItems.records.stream()
          .map(item -> HoldingsSupport.determineEffectiveLocationIdForItem(
            HoldingsSupport.holdingForItem(item, holdings).orElse(null), item))
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        effectiveLocationIds.stream().forEach(id -> {
          CompletableFuture<Response> newFuture = new CompletableFuture<>();

          allFutures.add(newFuture);
          allEffectiveLocationsFutures.add(newFuture);

          effectiveLocationsClient.get(id, newFuture::complete);
        });

        List<String> permanentLocationIds = wrappedItems.records.stream()
          .map(item -> item.getPermanentLocationId())
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLocationIds = wrappedItems.records.stream()
          .map(item -> item.getTemporaryLocationId())
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
          log.info("GET all items: all futures completed");

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

            Map<String, JsonObject> foundEffectiveLocations
              = allEffectiveLocationsFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r-> r));

            Map<String, JsonObject> foundLocations
              = allLocationsFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

            JsonResponse.success(routingContext.response(),
              new ItemRepresentation(RELATIVE_ITEMS_PATH)
                .toJson(wrappedItems, holdings, instances, foundMaterialTypes,
                  foundLoanTypes, foundLocations, foundEffectiveLocations, context));
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

    return createCollectionResourceClient(client, context, "/material-types");
  }

  private CollectionResourceClient createLoanTypesClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context, "/loan-types");
  }

  private CollectionResourceClient createLocationsClient(
    OkapiHttpClient client,
    WebContext context)
    throws MalformedURLException {

    return createCollectionResourceClient(client, context, "/locations");
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
    JsonObject holding,
    JsonObject instance,
    CompletableFuture<Response> materialTypeFuture,
    String effectiveLocationId,
    CompletableFuture<Response> permanentLoanTypeFuture,
    CompletableFuture<Response> temporaryLoanTypeFuture,
    CompletableFuture<Response> temporaryLocationFuture,
    CompletableFuture<Response> permanentLocationFuture,
    CompletableFuture<Response> effectiveLocationFuture) {

    JsonObject foundMaterialType =
      referenceRecordFrom(item.getMaterialTypeId(), materialTypeFuture);

    JsonObject foundPermanentLoanType =
      referenceRecordFrom(item.getPermanentLoanTypeId(), permanentLoanTypeFuture);

    JsonObject foundTemporaryLoanType =
      referenceRecordFrom(item.getTemporaryLoanTypeId(), temporaryLoanTypeFuture);

    JsonObject foundPermanentLocation =
      referenceRecordFrom(item.getPermanentLocationId(), permanentLocationFuture);

    JsonObject foundTemporaryLocation =
      referenceRecordFrom(item.getTemporaryLocationId(), temporaryLocationFuture);

    JsonObject foundEffectiveLocation =
      referenceRecordFrom(effectiveLocationId, effectiveLocationFuture);

    return new ItemRepresentation(RELATIVE_ITEMS_PATH)
        .toJson(item,
          holding,
          instance,
          foundMaterialType,
          foundPermanentLoanType,
          foundTemporaryLoanType,
          foundPermanentLocation,
          foundTemporaryLocation,
          foundEffectiveLocation,
          context);
  }

  private boolean hasSameBarcode(Item updatedItem, Item foundItem) {
    return updatedItem.getBarcode() == null
      || Objects.equals(foundItem.getBarcode(), updatedItem.getBarcode());
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
      CqlHelper.barcodeIs(updatedItem.getBarcode()) + " and id<>" + updatedItem.id,
      PagingParameters.defaults(), it -> {

        List<Item> items = it.getResult().records;

        if(items.isEmpty()) {
          updateItem(routingContext, updatedItem, itemCollection);
        }
        else {
          ClientErrorResponse.badRequest(routingContext.response(),
            String.format("Barcode must be unique, %s is already assigned to another item",
              updatedItem.getBarcode()));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

}


