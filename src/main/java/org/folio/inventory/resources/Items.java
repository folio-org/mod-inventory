package org.folio.inventory.resources;

import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.inventory.common.FutureAssistance.allOf;
import static org.folio.inventory.support.CqlHelper.multipleRecordsCqlQuery;
import static org.folio.inventory.support.EndpointFailureHandler.doExceptionally;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.ItemStatusValidator.itemHasCorrectStatus;
import static org.folio.inventory.validation.ItemsValidator.claimedReturnedMarkedAsMissing;
import static org.folio.inventory.validation.ItemsValidator.hridChanged;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;
import org.folio.inventory.services.MoveItemIntoStatusService;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.Clients;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.ForwardResponse;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.support.http.server.SuccessResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.inventory.validation.ItemsValidator;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;


public class Items extends AbstractInventoryResource {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private static final String RELATIVE_ITEMS_PATH = "/inventory/items";
  private static final String RELATIVE_ITEMS_PATH_ID = RELATIVE_ITEMS_PATH+"/:id";


  private static final int STATUS_CREATED = 201;
  private static final int STATUS_SUCCESS = 200;

  private final DateTimeFormatter dateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  public Items(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.post(RELATIVE_ITEMS_PATH + "*").handler(BodyHandler.create());
    router.put(RELATIVE_ITEMS_PATH + "*").handler(BodyHandler.create());

    router.get(RELATIVE_ITEMS_PATH).handler(this::getAll);
    router.post(RELATIVE_ITEMS_PATH).handler(this::create);
    router.delete(RELATIVE_ITEMS_PATH).handler(this::deleteAll);

    router.get(RELATIVE_ITEMS_PATH_ID).handler(this::getById);
    router.put(RELATIVE_ITEMS_PATH_ID).handler(this::update);
    router.delete(RELATIVE_ITEMS_PATH_ID).handler(this::deleteById);

    Arrays.stream(ItemStatusName.values()).map(itemStatusName -> ItemStatusURL.getUrlForItemStatusName(itemStatusName))
      .filter(itemStatusUrl -> itemStatusUrl.isPresent())
      .forEach(itemStatusUrl -> registerMarkItemAsHandler(itemStatusUrl.get(), router));
  }

  private void registerMarkItemAsHandler(String itemStatusUrl, Router router) {
    router.post(RELATIVE_ITEMS_PATH_ID + itemStatusUrl)
      .handler(handle(this::markItemAsTargetStatus));
  }

  private CompletableFuture<Void> markItemAsTargetStatus(
    RoutingContext routingContext, WebContext webContext, Clients clients) {

    final var itemStatusName = ItemStatusURL.getItemStatusNameForUrl(routingContext.request().uri());
    if (itemStatusName.isEmpty())
      log.error("Item status for url $URL$ not found.".replace("$URL$", routingContext.request().uri()),
        new Exception());

    final MoveItemIntoStatusService moveItemIntoStatusService = new MoveItemIntoStatusService(storage
      .getItemCollection(webContext), clients);

    return moveItemIntoStatusService.markItemAs(itemStatusName.get(), webContext)
      .thenAccept(item -> respondWithItemRepresentation(item, HTTP_OK.toInt(),
        routingContext, webContext));
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

    Optional<ValidationError> validationError = itemHasCorrectStatus(item);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    Item newItem = ItemUtil.jsonToItem(item);

    ItemCollection itemCollection = storage.getItemCollection(context);
    UserCollection userCollection = storage.getUserCollection(context);

    if(newItem.getBarcode() != null) {
      try {
        itemCollection.findByCql(CqlHelper.barcodeIs(newItem.getBarcode()),
          PagingParameters.defaults(), findResult -> {

            if(findResult.getResult().records.isEmpty()) {
              findUserAndAddItem(routingContext, context, newItem, userCollection, itemCollection);
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
      findUserAndAddItem(routingContext, context, newItem, userCollection, itemCollection);
    }
  }

  private void update(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    JsonObject itemRequest = routingContext.getBodyAsJson();

    Optional<ValidationError> validationError = itemHasCorrectStatus(itemRequest);
    if (validationError.isPresent()) {
      unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    Item newItem = ItemUtil.jsonToItem(itemRequest);

    ItemCollection itemCollection = storage.getItemCollection(context);
    UserCollection userCollection = storage.getUserCollection(context);

    final String itemId = routingContext.request().getParam("id");
    final CompletableFuture<Success<Item>> getItemFuture = new CompletableFuture<>();

    itemCollection.findById(itemId, getItemFuture::complete,
      FailureResponseConsumer.serverError(routingContext.response()));

    getItemFuture
      .thenApply(Success::getResult)
      .thenCompose(ItemsValidator::refuseWhenItemNotFound)
      .thenCompose(oldItem -> hridChanged(oldItem, newItem))
      .thenCompose(oldItem -> claimedReturnedMarkedAsMissing(oldItem, newItem))
      .thenAccept(oldItem -> {
        if (hasSameBarcode(newItem, oldItem)) {
          findUserAndUpdateItem(routingContext, newItem, oldItem, userCollection, itemCollection);
        } else {
          try {
            checkForNonUniqueBarcode(routingContext, newItem, oldItem, itemCollection, userCollection);
          } catch (UnsupportedEncodingException e) {
            ServerErrorResponse.internalError(routingContext.response(), e.toString());
          }
        }
      }).exceptionally(doExceptionally(routingContext));
  }

  private void deleteById(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    CollectionResourceClient itemsStorageClient;

    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
      itemsStorageClient = createItemsStorageClient(okapiClient, context);
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

    storage.getItemCollection(context).findById(
      routingContext.request().getParam("id"),
      (Success<Item> itemResponse) -> {
        Item item = itemResponse.getResult();

        if(item != null) {
          respondWithItemRepresentation(item, STATUS_SUCCESS, routingContext, context);
        }
        else {
          ClientErrorResponse.notFound(routingContext.response());
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  protected void respondWithManyItems(
    RoutingContext routingContext,
    WebContext context,
    MultipleRecords<Item> wrappedItems) {

    CollectionResourceClient holdingsClient;
    CollectionResourceClient instancesClient;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;
    CollectionResourceClient boundWithPartsClient;

    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
      holdingsClient = createHoldingsClient(okapiClient, context);
      instancesClient = createInstancesClient(okapiClient, context);
      materialTypesClient = createMaterialTypesClient(okapiClient, context);
      loanTypesClient = createLoanTypesClient(okapiClient, context);
      locationsClient = createLocationsClient(okapiClient, context);
      boundWithPartsClient = createBoundWithPartsClient(okapiClient, context);
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
      .map(Item::getHoldingId)
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
          .map(Item::getMaterialTypeId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        materialTypeIds.forEach(id -> {
          CompletableFuture<Response> newFuture = new CompletableFuture<>();

          allFutures.add(newFuture);
          allMaterialTypeFutures.add(newFuture);

          materialTypesClient.get(id, newFuture::complete);
        });

        List<String> permanentLoanTypeIds = wrappedItems.records.stream()
          .map(Item::getPermanentLoanTypeId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLoanTypeIds = wrappedItems.records.stream()
          .map(Item::getTemporaryLoanTypeId)
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
          .map(Item::getEffectiveLocationId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> permanentLocationIds = wrappedItems.records.stream()
          .map(Item::getPermanentLocationId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        List<String> temporaryLocationIds = wrappedItems.records.stream()
          .map(Item::getTemporaryLocationId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

        Stream.concat(Stream.concat(permanentLocationIds.stream(), temporaryLocationIds.stream()), effectiveLocationIds.stream())
          .distinct()
          .forEach(id -> {

            CompletableFuture<Response> newFuture = new CompletableFuture<>();

            allFutures.add(newFuture);
            allLocationsFutures.add(newFuture);

            locationsClient.get(id, newFuture::complete);
          });

        CompletableFuture<Response> boundWithPartsFuture =
          getBoundWithPartsForMultipleItemsFuture(wrappedItems, boundWithPartsClient);
        allFutures.add(boundWithPartsFuture);

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

            Map<String, JsonObject> foundLocations
              = allLocationsFutures.stream()
              .map(CompletableFuture::join)
              .filter(response -> response.getStatusCode() == 200)
              .map(Response::getJson)
              .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

            setBoundWithFlagsOnItems(wrappedItems, boundWithPartsFuture);

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

    return new OkapiHttpClient(WebClient.wrap(client), context,
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

  private CollectionResourceClient createBoundWithPartsClient(
    OkapiHttpClient client,
    WebContext webContext)
    throws MalformedURLException {
    return createCollectionResourceClient(client, webContext, "/inventory-storage/bound-with-parts");
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

  private void findUserAndAddItem(
    RoutingContext routingContext,
    WebContext webContext,
    Item newItem,
    UserCollection userCollection,
    ItemCollection itemCollection) {

    String userId = webContext.getUserId();
    userCollection.findById(userId,
      success -> addItem(routingContext, webContext, newItem, success.getResult(), itemCollection),
      failure -> addItem(routingContext, webContext, newItem, null, itemCollection));
  }

  private void addItem(
    RoutingContext routingContext,
    WebContext webContext,
    Item newItem,
    User user,
    ItemCollection itemCollection) {

    List<CirculationNote> notes = newItem.getCirculationNotes()
      .stream()
      .map(note -> note.withId(UUID.randomUUID().toString()))
      .map(note -> note.withSource(user))
      .map(note -> note.withDate(dateTimeFormatter.format(ZonedDateTime.now())))
      .collect(Collectors.toList());

    itemCollection.add(newItem.withCirculationNotes(notes), success -> {
      Item item = success.getResult();
      respondWithItemRepresentation(item, STATUS_CREATED, routingContext, webContext);
    }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private void respondWithItemRepresentation (
          Item item, int responseStatus, RoutingContext routingContext, WebContext webContext)
  {
    CollectionResourceClient holdingsClient;
    CollectionResourceClient instancesClient;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;
    CollectionResourceClient boundWithPartsClient;

    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, webContext);
      holdingsClient = createHoldingsClient(okapiClient, webContext);
      instancesClient = createInstancesClient(okapiClient, webContext);
      materialTypesClient = createMaterialTypesClient(okapiClient, webContext);
      loanTypesClient = createLoanTypesClient(okapiClient, webContext);
      locationsClient = createLocationsClient(okapiClient, webContext);
      boundWithPartsClient = createBoundWithPartsClient(okapiClient, webContext);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, webContext);
      return;
    }
    holdingsClient.get(item.getHoldingId(), (Response holdingResponse) -> {
      final JsonObject holding = holdingResponse.getStatusCode() == 200
        ? holdingResponse.getJson()
        : null;

      String instanceId = holdingResponse.getStatusCode() == 200
        ? holdingResponse.getJson().getString("instanceId")
        : null;

      instancesClient.get(instanceId, (Response instanceResponse) -> {
        final JsonObject instance = instanceResponse.getStatusCode() == 200
          ? instanceResponse.getJson()
          : null;

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
          item.getEffectiveLocationId(), locationsClient, allFutures);

        allFutures.add(
          setBoundWithTitlesOnItem( item,
            boundWithPartsClient, holdingsClient, instancesClient));

        CompletableFuture<Void> allDoneFuture = allOf(allFutures);

        allDoneFuture.thenAccept(v -> {
          try {
            JsonObject representation = includeReferenceRecordInformationInItem(
                      webContext, item, holding, instance,
                      materialTypeFuture,
                      permanentLoanTypeFuture,
                      temporaryLoanTypeFuture,
                      temporaryLocationFuture,
                      permanentLocationFuture,
                      effectiveLocationFuture);

            switch (responseStatus) {
              case STATUS_CREATED :
                JsonResponse.created(routingContext.response(), representation);
                break;
              case STATUS_SUCCESS :
                JsonResponse.success(routingContext.response(), representation);
                break;
              default:
                ServerErrorResponse.internalError(routingContext.response(),
                  "System specified invalid status code for Item response");
                break;
            }
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(),
              String.format("Error responding with Item representation: %s", e));
          }
        });
      });
    });
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
      referenceRecordFrom(item.getEffectiveLocationId(), effectiveLocationFuture);

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

  private void findUserAndUpdateItem(
    RoutingContext routingContext,
    Item newItem,
    Item oldItem,
    UserCollection userCollection,
    ItemCollection itemCollection) {

    String userId = routingContext.request().getHeader("X-Okapi-User-Id");
    userCollection.findById(userId,
      success -> updateItem(routingContext, newItem, oldItem, success.getResult(), itemCollection),
      failure -> updateItem(routingContext, newItem, oldItem, null, itemCollection));
  }

  private void updateItem(
    RoutingContext routingContext,
    Item newItem,
    Item oldItem,
    User user,
    ItemCollection itemCollection) {

    Map<String, CirculationNote> oldNotes = oldItem.getCirculationNotes()
      .stream()
      .collect(Collectors.toMap(CirculationNote::getId, Function.identity()));

    List<CirculationNote> updatedNotes = newItem.getCirculationNotes()
      .stream()
      .map(note -> updateCirculationNoteIfChanged(note, user, oldNotes))
      .collect(Collectors.toList());

    itemCollection.update(newItem.withCirculationNotes(updatedNotes),
      v -> SuccessResponse.noContent(routingContext.response()),
      failure -> ForwardResponse.forward(routingContext.response(), failure));
  }

  private void checkForNonUniqueBarcode(
    RoutingContext routingContext,
    Item newItem,
    Item oldItem,
    ItemCollection itemCollection,
    UserCollection userCollection)
    throws UnsupportedEncodingException {

    itemCollection.findByCql(
      CqlHelper.barcodeIs(newItem.getBarcode()) + " and id<>" + newItem.id,
      PagingParameters.defaults(), it -> {

        List<Item> items = it.getResult().records;

        if(items.isEmpty()) {
          findUserAndUpdateItem(routingContext, newItem, oldItem, userCollection, itemCollection);
        }
        else {
          ClientErrorResponse.badRequest(routingContext.response(),
            String.format("Barcode must be unique, %s is already assigned to another item",
              newItem.getBarcode()));
        }
      }, FailureResponseConsumer.serverError(routingContext.response()));
  }

  private CirculationNote updateCirculationNoteIfChanged(CirculationNote newNote,
                                                         User user, Map<String, CirculationNote> oldNotes) {
    String noteId = newNote.getId();

    if (noteId == null) {
      return newNote.withId(UUID.randomUUID().toString())
        .withSource(user)
        .withDate(dateTimeFormatter.format(ZonedDateTime.now()));
    } else if (circulationNoteChanged(newNote, oldNotes.get(noteId))) {
      return newNote.withSource(user)
        .withDate(dateTimeFormatter.format(ZonedDateTime.now()));
    } else {
      return newNote;
    }
  }

  private boolean circulationNoteChanged(CirculationNote newNote, CirculationNote oldNote) {
    if (!newNote.getNoteType().equals(oldNote.getNoteType())) {
      return true;
    } else if (!newNote.getNote().equals(oldNote.getNote())) {
      return true;
    } else {
      return !newNote.getStaffOnly().equals(oldNote.getStaffOnly());
    }
  }

  /**
   * Fetches bound-with parts and referenced holdingsRecords and Instances
   * and builds a list of bound-with titles that is set on the provided Item.
   * NOTE: This method will mutate the argument item (adding data to it)
   *
   * @param item The Item to set bound-with titles on
   * @param boundWithPartsClient Client for retrieving bound-with parts from storage
   * @param holdingsClient Client for retrieving holdings from storage
   * @param instancesClient Client for retrieving Instance from storage
   * @return Future with null Response (Using Response return type in order to
   * add this future to a list of other futures returning Response)
   */
  private CompletableFuture<Response> setBoundWithTitlesOnItem(
    Item item,
    CollectionResourceClient boundWithPartsClient,
    CollectionResourceClient holdingsClient,
    CollectionResourceClient instancesClient
  ) {
    return getBoundWithPartsForItemFuture( item, boundWithPartsClient ).thenCompose( partsResponse -> {
      JsonArray boundWithParts = partsResponse.getJson().getJsonArray("boundWithParts" );
      if ( boundWithParts.isEmpty() ) {
        item.withIsBoundWith( false );
        return CompletableFuture.completedFuture( null );
      } else {
        return fetchHoldingsByForeignKeys( boundWithParts, holdingsClient ).thenCompose(
          holdingsRecords -> fetchInstancesByForeignKeys( holdingsRecords, instancesClient ).thenCompose(
            instances -> {
              JsonArray boundWithTitles = buildBoundWithTitlesArray( holdingsRecords, instances );
              item.withBoundWithTitles( boundWithTitles ).withIsBoundWith( true );
              return CompletableFuture.completedFuture( null );
            } ) );
      }});
  }

  /**
   * Constructs a JSON array of boundWithTitles containing Instance and
   * holdingsRecord information
   * @param holdingsRecords The holdings records that should populate the array
   * @param instances The Instances that should populate the array
   * @return JSON array of boundWithTitles
   */
  private JsonArray buildBoundWithTitlesArray (JsonArray holdingsRecords, JsonArray instances) {
    JsonArray boundWithTitles = new JsonArray();

    Map<String, JsonObject> instancesByIdMap = new HashMap();
    instances.stream().forEach( instance -> {
      instancesByIdMap.put( ( (JsonObject) instance ).getString( "id" ),
        (JsonObject) instance );
    } );

    holdingsRecords.stream().forEach( record -> {
      JsonObject holdingsRecord = (JsonObject) record;
      JsonObject boundWithTitle = new JsonObject();
      JsonObject briefHoldingsRecord = new JsonObject();
      JsonObject briefInstance = new JsonObject();
      String instanceId = holdingsRecord.getString( "instanceId" );
      briefHoldingsRecord.put( "id", holdingsRecord.getString( "id" ) );
      briefHoldingsRecord.put( "hrid", holdingsRecord.getString( "hrid" ) );
      briefInstance.put( "id", instanceId );
      briefInstance.put( "title", instancesByIdMap.get( instanceId ).getString( "title" ) );
      briefInstance.put( "hrid", instancesByIdMap.get( instanceId ).getString( "hrid" ) );
      boundWithTitle.put( "briefHoldingsRecord", briefHoldingsRecord );
      boundWithTitle.put( "briefInstance", briefInstance );
      boundWithTitles.add( boundWithTitle );
    } );
    return boundWithTitles;
  }

  /**
   * For a set of entities with a 'holdingsRecordId' property, this method fetches holdingsRecords from storage by those holdingsRecordIds
   * @param entities  Array of records with a property named 'holdingsRecordId'
   * @param holdingsClient Client for fetching holdings records from storage
   * @return Array of holdings records found by provided entities' holdingsRecordIds
   */
  private CompletableFuture<JsonArray> fetchHoldingsByForeignKeys( JsonArray entities, CollectionResourceClient holdingsClient) {
    List<String> holdingsRecordIds = getStringPropertyFromJsonArray(
      entities, "holdingsRecordId" );
    CompletableFuture<Response> holdingsFetched = new CompletableFuture<>();
    holdingsClient.getMany( multipleRecordsCqlQuery( holdingsRecordIds ),
      holdingsRecordIds.size(), 0, holdingsFetched::complete );
    return holdingsFetched.thenCompose( holdingsResponse -> {
      JsonArray holdingsRecords = holdingsResponse.getJson().getJsonArray(
        "holdingsRecords" );
      return CompletableFuture.completedFuture( holdingsRecords );
    });
  }

  /**
   * "Joins" Instances from storage with a JSON array of entities with 'instanceId's
   * @param entities  Array of records with a property named 'instanceId'
   * @param instancesClient Client for fetching Instances from storage
   * @return Array of Instances found by the provided entities' instanceIds
   */
  private CompletableFuture<JsonArray> fetchInstancesByForeignKeys( JsonArray entities, CollectionResourceClient instancesClient) {
    List<String> instanceIds = getStringPropertyFromJsonArray(
      entities, "instanceId" );
    CompletableFuture<Response> instancesFetched = new CompletableFuture<>();
    instancesClient.getMany( multipleRecordsCqlQuery( instanceIds ),
      instanceIds.size(), 0, instancesFetched::complete );
    return instancesFetched.thenCompose( instancesResponse -> {
      JsonArray instances = instancesResponse.getJson().getJsonArray(
        "instances" );
      return CompletableFuture.completedFuture( instances );
    });
  }

  /**
   * Picks select String property from JSON array of records
   * @param entities  The records to pick the property from
   * @param propertyName Name of the property to retrieve
   * @return List of values for the provided property name
   */
  private List<String> getStringPropertyFromJsonArray( JsonArray entities, String propertyName) {
    return entities.stream()
      .map(o -> ((JsonObject)o).getString(propertyName))
      .collect( Collectors.toList());
  }

  private CompletableFuture<Response> getBoundWithPartsForItemFuture(
    Item item,
    CollectionResourceClient boundWithPartsClient)
  {

    CompletableFuture<Response> future = new CompletableFuture<>();

    String boundWithPartsByItemIdQuery = String.format("itemId==(%s)",
      item.getId());

    boundWithPartsClient.getMany(
      boundWithPartsByItemIdQuery,
      1000,
      0,
      future::complete);

    return future;
  }

  private void setBoundWithFlagsOnItems(MultipleRecords<Item> wrappedItems,
                                        CompletableFuture<Response> boundWithPartsFuture) {

    Response response = boundWithPartsFuture.join();
    if (response != null && response.hasBody() && response.getStatusCode()==200) {
      JsonArray boundWithParts = response.getJson().getJsonArray("boundWithParts");
      if (boundWithParts != null && !boundWithParts.isEmpty()) {
        Set<String> boundWithItemIds = boundWithParts
          .stream()
          .map(o -> ((JsonObject) o).getString("itemId"))
          .collect(Collectors.toSet());

        for (Item item : wrappedItems.records) {
          if (boundWithItemIds.contains(item.getId())) {
            item.withIsBoundWith(true);
          }
        }
      }
    } else {
      log.error("Failed to retrieve bound-with parts, status code:  " + (response != null ? response.getStatusCode() : "null response"));
    }

  }

  private CompletableFuture<Response> getBoundWithPartsForMultipleItemsFuture(
    MultipleRecords<Item> wrappedItems,
    CollectionResourceClient boundWithPartsClient)
  {
    CompletableFuture<Response> future = new CompletableFuture<>();

    List<String> itemIds = wrappedItems.records.stream()
      .map(Item::getId)
      .collect(Collectors.toList());

    String boundWithPartsByItemIdsQuery =
      String.format("itemId==(%s)",
        itemIds.stream()
          .map(String::toString)
          .collect(Collectors.joining(" or ")));

    boundWithPartsClient.getMany(
      boundWithPartsByItemIdsQuery,
      itemIds.size(),
      0,
      future::complete);

    return future;
  }

}


