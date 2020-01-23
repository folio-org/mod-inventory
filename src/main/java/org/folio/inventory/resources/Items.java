package org.folio.inventory.resources;

import static org.folio.inventory.common.FutureAssistance.allOf;
import static org.folio.inventory.support.CqlHelper.multipleRecordsCqlQuery;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.folio.inventory.validation.ItemStatusValidator.itemHasCorrectStatus;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.LastCheckIn;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.CqlHelper;
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

import io.vertx.core.http.HttpClient;
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
  private static final int STATUS_CREATED = 201;
  private static final int STATUS_SUCCESS = 200;

  private final HttpClient client;

  private final DateTimeFormatter dateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  public Items(final Storage storage, final HttpClient client) {
    this.storage = storage;
    this.client = client;
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

    Optional<ValidationError> validationError = itemHasCorrectStatus(item);
    if (validationError.isPresent()) {
      JsonResponse.unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    Item newItem = requestToItem(item);

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
      JsonResponse.unprocessableEntity(routingContext.response(), validationError.get());
      return;
    }

    Item newItem = requestToItem(itemRequest);

    ItemCollection itemCollection = storage.getItemCollection(context);
    UserCollection userCollection = storage.getUserCollection(context);

    itemCollection.findById(routingContext.request().getParam("id"), getItemResult -> {
      Item oldItem = getItemResult.getResult();
      if (oldItem != null) {
        if (!Objects.equals(newItem.getHrid(), oldItem.getHrid())) {
          log.warn("The HRID property can not be updated, old value is '{}' but new is '{}'",
            oldItem.getHrid(), newItem.getHrid()
          );

          JsonResponse.unprocessableEntity(routingContext.response(),
            "HRID can not be updated", "hrid", newItem.getHrid()
          );
          return;
        }

        if (hasSameBarcode(newItem, oldItem)) {
          findUserAndUpdateItem(routingContext, newItem, oldItem, userCollection, itemCollection);
        } else {
          try {
            checkForNonUniqueBarcode(routingContext, newItem, oldItem, itemCollection, userCollection);
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

  private Item requestToItem(JsonObject itemRequest) {
    List<String> formerIds = toListOfStrings(
      itemRequest.getJsonArray(Item.FORMER_IDS_KEY));

    List<String> statisticalCodeIds = toListOfStrings(
      itemRequest.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));

    List<String> yearCaption = toListOfStrings(
      itemRequest.getJsonArray(Item.YEAR_CAPTION_KEY));

    Status status = new Status(itemRequest.getJsonObject(Item.STATUS_KEY));

    List<Note> notes = itemRequest.containsKey(Item.NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.NOTES_KEY)).stream()
          .map(json -> new Note(json))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<CirculationNote> circulationNotes = itemRequest.containsKey(Item.CIRCULATION_NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.CIRCULATION_NOTES_KEY)).stream()
          .map(json -> new CirculationNote(json))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<ElectronicAccess> electronicAccess = itemRequest.containsKey(Item.ELECTRONIC_ACCESS_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.ELECTRONIC_ACCESS_KEY)).stream()
          .map(json -> new ElectronicAccess(json))
          .collect(Collectors.toList())
          : new ArrayList<>();

    List<String> tags = itemRequest.containsKey(Item.TAGS_KEY)
      ? getTags(itemRequest) : new ArrayList<>();


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
            .withHrid(itemRequest.getString(Item.HRID_KEY))
            .withFormerIds(formerIds)
            .withDiscoverySuppress(itemRequest.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
            .withBarcode(itemRequest.getString("barcode"))
            .withItemLevelCallNumber(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
            .withItemLevelCallNumberPrefix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
            .withItemLevelCallNumberSuffix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
            .withItemLevelCallNumberTypeId(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
            .withVolume(itemRequest.getString(Item.VOLUME_KEY))
            .withEnumeration(itemRequest.getString("enumeration"))
            .withChronology(itemRequest.getString("chronology"))
            .withNumberOfPieces(itemRequest.getString("numberOfPieces"))
            .withDescriptionOfPieces(itemRequest.getString(Item.DESCRIPTION_OF_PIECES_KEY))
            .withNumberOfMissingPieces(itemRequest.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
            .withMissingPieces(itemRequest.getString(Item.MISSING_PIECES_KEY))
            .withMissingPiecesDate(itemRequest.getString(Item.MISSING_PIECES_DATE_KEY))
            .withItemDamagedStatusId(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
            .withItemDamagedStatusDate(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
            .withPermanentLocationId(permanentLocationId)
            .withTemporaryLocationId(temporaryLocationId)
            .withTemporaryLoanTypeId(temporaryLoanTypeId)
            .withCopyNumber(itemRequest.getString(Item.COPY_NUMBER_KEY))
            .withNotes(notes)
            .withCirculationNotes(circulationNotes)
            .withAccessionNumber(itemRequest.getString(Item.ACCESSION_NUMBER_KEY))
            .withItemIdentifier(itemRequest.getString(Item.ITEM_IDENTIFIER_KEY))
            .withYearCaption(yearCaption)
            .withElectronicAccess(electronicAccess)
            .withStatisticalCodeIds(statisticalCodeIds)
            .withPurchaseOrderLineidentifier(itemRequest.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))
            .withLastCheckIn(LastCheckIn.from(itemRequest.getJsonObject(Item.LAST_CHECK_IN)))
            .withTags(tags);
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
      OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
      holdingsClient = createHoldingsClient(okapiClient, context);
      instancesClient = createInstancesClient(okapiClient, context);
      materialTypesClient = createMaterialTypesClient(okapiClient, context);
      loanTypesClient = createLoanTypesClient(okapiClient, context);
      locationsClient = createLocationsClient(okapiClient, context);
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
          .map(Item::getEffectiveLocationId)
          .filter(Objects::nonNull)
          .distinct()
          .collect(Collectors.toList());

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

        Stream.concat(Stream.concat(permanentLocationIds.stream(), temporaryLocationIds.stream()), effectiveLocationIds.stream())
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

    return new OkapiHttpClient(client, context,
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

  private List<String> getTags(JsonObject itemRequest) {
    final JsonObject tags = itemRequest.getJsonObject(Item.TAGS_KEY);
    return tags.containsKey(Item.TAG_LIST_KEY) ?
      JsonArrayHelper.toListOfStrings(tags.getJsonArray(Item.TAG_LIST_KEY)) : new ArrayList<>();
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

    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, webContext);
      holdingsClient = createHoldingsClient(okapiClient, webContext);
      instancesClient = createInstancesClient(okapiClient, webContext);
      materialTypesClient = createMaterialTypesClient(okapiClient, webContext);
      loanTypesClient = createLoanTypesClient(okapiClient, webContext);
      locationsClient = createLocationsClient(okapiClient, webContext);
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
      failure -> ServerErrorResponse.internalError(
        routingContext.response(), failure.getReason()));
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
}


