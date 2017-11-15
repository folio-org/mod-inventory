package org.folio.inventory.resources.ingest;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.CollectionResourceClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.ingest.IngestMessages;
import org.folio.inventory.parsing.ModsParser;
import org.folio.inventory.parsing.UTF8LiteralCharacterEncoding;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ModsIngestion {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String RELATIVE_MODS_INGEST_PATH = "/inventory/ingest/mods";

  private final Storage storage;

  public ModsIngestion(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(RELATIVE_MODS_INGEST_PATH + "*").handler(BodyHandler.create());
    router.post(RELATIVE_MODS_INGEST_PATH).handler(this::ingest);
    router.get(RELATIVE_MODS_INGEST_PATH + "/status/:id").handler(this::status);
  }

  //TODO: Will only work for single examples of each reference record
  // book material type
  // can circulate loan type
  // main library location
  // books instance type
  // isbn identifier type
  // personal creator type
  private void ingest(RoutingContext routingContext) {
    if(routingContext.fileUploads().size() > 1) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Cannot parse multiple files in a single request");
      return;
    }

    WebContext context = new WebContext(routingContext);
    OkapiHttpClient client;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;
    CollectionResourceClient instanceTypesClient;
    CollectionResourceClient identifierTypesClient;
    CollectionResourceClient creatorTypesClient;

    try {
      client = createHttpClient(routingContext, context);

      materialTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/material-types"));

      loanTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/loan-types"));

      locationsClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/shelf-locations"));

      identifierTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/identifier-types"));

      instanceTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/instance-types"));

      creatorTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/creator-types"));
    }
    catch (MalformedURLException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

      return;
    }

    String materialTypesQuery = null;
    String loanTypesQuery = null;
    String locationsQuery = null;
    String instanceTypesQuery = null;
    String identifierTypesQuery = null;
    String creatorTypesQuery = null;

    try {
      materialTypesQuery = getReferenceRecordQuery("Book");
      loanTypesQuery = getReferenceRecordQuery("Can Circulate");
      locationsQuery = getReferenceRecordQuery("Main Library");
      instanceTypesQuery = getReferenceRecordQuery("Books");
      identifierTypesQuery = getReferenceRecordQuery("ISBN");
      creatorTypesQuery = getReferenceRecordQuery("Personal name");

    } catch (UnsupportedEncodingException e) {
      String error = String.format("Failed to encode query: %s", e.toString());

      log.error(error);
      ServerErrorResponse.internalError(routingContext.response(), error);
    }

    CompletableFuture<Response> materialTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> loanTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> locationsRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> instanceTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> identifierTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> creatorTypesRequestCompleted = new CompletableFuture<>();

    materialTypesClient.getMany(
      materialTypesQuery,
      materialTypesRequestCompleted::complete);

    loanTypesClient.getMany(
      loanTypesQuery,
      loanTypesRequestCompleted::complete);

    locationsClient.getMany(
      locationsQuery,
      locationsRequestCompleted::complete);

    instanceTypesClient.getMany(
      instanceTypesQuery,
      instanceTypesRequestCompleted::complete);

    identifierTypesClient.getMany(
      identifierTypesQuery,
      identifierTypesRequestCompleted::complete);

    creatorTypesClient.getMany(
      creatorTypesQuery,
      creatorTypesRequestCompleted::complete);

    CompletableFuture.allOf(materialTypesRequestCompleted,
      loanTypesRequestCompleted, locationsRequestCompleted,
      instanceTypesRequestCompleted, identifierTypesRequestCompleted,
      creatorTypesRequestCompleted)
      .thenAccept(v -> {
        Response materialTypeResponse = materialTypesRequestCompleted.join();
        Response loanTypeResponse = loanTypesRequestCompleted.join();
        Response locationsResponse = locationsRequestCompleted.join();
        Response instanceTypesResponse = instanceTypesRequestCompleted.join();
        Response identifierTypesResponse = identifierTypesRequestCompleted.join();
        Response creatorTypesResponse = creatorTypesRequestCompleted.join();

      if (materialTypeResponse.getStatusCode() != 200) {
        ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve material types: %s: %s",
          materialTypeResponse.getStatusCode(), materialTypeResponse.getBody()));

      return;
    }

    if (loanTypeResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve loan types: %s: %s",
          loanTypeResponse.getStatusCode(), loanTypeResponse.getBody()));

      return;
    }

    if (locationsResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve locations: %s: %s",
          locationsResponse.getStatusCode(), locationsResponse.getBody()));

      return;
    }

    if (instanceTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve instance types: %s: %s",
          instanceTypesResponse.getStatusCode(), instanceTypesResponse.getBody()));

      return;
    }

    if (identifierTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve identifier types: %s: %s",
          identifierTypesResponse.getStatusCode(), identifierTypesResponse.getBody()));

      return;
    }

    if (creatorTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve creator types: %s: %s",
          creatorTypesResponse.getStatusCode(), creatorTypesResponse.getBody()));

      return;
    }

    List<JsonObject> materialTypes = JsonArrayHelper.toList(
      materialTypeResponse.getJson().getJsonArray("mtypes"));

    if(materialTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find book material type: %s", materialTypeResponse.getBody()));

      return;
    }

    List<JsonObject> loanTypes = JsonArrayHelper.toList(
      loanTypeResponse.getJson().getJsonArray("loantypes"));

    if(loanTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find can circulate loan type: %s", loanTypeResponse.getBody()));

      return;
    }

    List<JsonObject> locations = JsonArrayHelper.toList(
      locationsResponse.getJson().getJsonArray("shelflocations"));

    String bookMaterialTypeId = materialTypes.stream().findFirst().get().getString("id");
    String canCirculateLoanTypeId = loanTypes.stream().findFirst().get().getString("id");

    //location is optional so carry on gracefully
    String mainLibraryLocationId = locations.size() == 1
      ? locations.stream().findFirst().get().getString("id")
      : null;

    List<JsonObject> identifierTypes = JsonArrayHelper.toList(
      identifierTypesResponse.getJson().getJsonArray("identifierTypes"));

    if(identifierTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find ISBN identifier type: %s",
          identifierTypesResponse.getBody()));

      return;
    }

    String isbnIdentifierTypeId = identifierTypes.stream().findFirst().get().getString("id");

    List<JsonObject> instanceTypes = JsonArrayHelper.toList(
      instanceTypesResponse.getJson().getJsonArray("instanceTypes"));

    if(instanceTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find books instance type: %s",
          instanceTypesResponse.getBody()));

      return;
    }

    String booksInstanceTypeId = instanceTypes.stream().findFirst().get().getString("id");

    List<JsonObject> creatorTypes = JsonArrayHelper.toList(
      creatorTypesResponse.getJson().getJsonArray("creatorTypes"));

    if(creatorTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find personal name creator type: %s",
          creatorTypesResponse.getBody()));

      return;
    }

    String personalCreatorTypeId = creatorTypes.stream().findFirst().get().getString("id");

    String uploadFileName = uploadFileName(routingContext);

    if(StringUtils.isBlank(uploadFileName)) {
      ServerErrorResponse.internalError(routingContext.response(),
        "Unable to get upload file name");

      return;
    }

    routingContext.vertx().fileSystem().readFile(uploadFileName,
      result -> {
        if (result.succeeded()) {
          String uploadedFileContents = result.result().toString();

          try {
            List<JsonObject> records = new ModsParser(new UTF8LiteralCharacterEncoding())
              .parseRecords(uploadedFileContents);

            storage.getIngestJobCollection(context)
              .add(new IngestJob(IngestJobState.REQUESTED),
                success -> {
                  IngestMessages.start(records,
                    singleEntryMap("Book", bookMaterialTypeId),
                    singleEntryMap("Can Circulate", canCirculateLoanTypeId),
                    singleEntryMap("Main Library", mainLibraryLocationId),
                    singleEntryMap("ISBN", isbnIdentifierTypeId),
                    singleEntryMap("Books", booksInstanceTypeId),
                    singleEntryMap("Personal name", personalCreatorTypeId),
                    success.getResult().id, context).send(routingContext.vertx());

                  RedirectResponse.accepted(routingContext.response(),
                    statusLocation(routingContext, success.getResult().id));
                },
                failure -> log.error("Creating Ingest Job failed")
              );
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(),
              String.format("Unable to parse MODS file:%s", e.toString()));
          }
        } else {
        ServerErrorResponse.internalError(
          routingContext.response(), result.cause().toString());
        }
      });
    });
  }

  private static String getReferenceRecordQuery(String name)
    throws UnsupportedEncodingException {

    return "query=" + URLEncoder.encode(String.format("name=\"%s\"", name), "UTF-8");
  }

  private void status(RoutingContext routingContext) {
    Context context = new WebContext(routingContext);

    storage.getIngestJobCollection(context)
      .findById(routingContext.request().getParam("id"),
        it -> JsonResponse.success(routingContext.response(),
          new JsonObject().put("status", it.getResult().state.toString())),
        FailureResponseConsumer.serverError(routingContext.response()));
  }

  private Map<String, String> singleEntryMap(String key, String value) {
    HashMap<String, String> map = new HashMap<>();

    map.put(key, value);

    return map;
  }

  private String statusLocation(RoutingContext routingContext, String jobId) {
    String scheme = routingContext.request().scheme();
    String host = routingContext.request().host();

    return String.format("%s://%s%s/status/%s",
      scheme, host, RELATIVE_MODS_INGEST_PATH, jobId);
  }

  private String uploadFileName(RoutingContext routingContext) {
    Optional<FileUpload> possibleUploadedFile = routingContext.fileUploads()
      .stream().findFirst();

    return possibleUploadedFile.isPresent()
      ? possibleUploadedFile.get().uploadedFileName()
      : null;
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
}
