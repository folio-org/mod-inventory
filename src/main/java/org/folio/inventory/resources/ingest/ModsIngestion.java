package org.folio.inventory.resources.ingest;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.ingest.IngestMessages;
import org.folio.inventory.parsing.ModsParser;
import org.folio.inventory.parsing.UTF8LiteralCharacterEncoding;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.ReferenceRecord;
import org.folio.inventory.storage.external.ReferenceRecordClient;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
  // personal contributor type
  private void ingest(RoutingContext routingContext) {
    if(routingContext.fileUploads().size() > 1) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Cannot parse multiple files in a single request");
      return;
    }

    WebContext context = new WebContext(routingContext);
    OkapiHttpClient client;
    ReferenceRecordClient materialTypesClient;
    ReferenceRecordClient loanTypesClient;
    ReferenceRecordClient locationsClient;
    ReferenceRecordClient instanceTypesClient;
    ReferenceRecordClient identifierTypesClient;
    ReferenceRecordClient contributorNameTypesClient;

    try {
      client = createHttpClient(routingContext, context);

      materialTypesClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/material-types")), "mtypes");

      loanTypesClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/loan-types")), "loantypes");

      locationsClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/shelf-locations")), "shelflocations");

      identifierTypesClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/identifier-types")), "identifierTypes");

      instanceTypesClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/instance-types")), "instanceTypes");

      contributorNameTypesClient = new ReferenceRecordClient(new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/contributor-name-types")), "contributorNameTypes");
    }
    catch (MalformedURLException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

      return;
    }

    CompletableFuture<ReferenceRecord> materialTypesRequestCompleted;
    CompletableFuture<ReferenceRecord> loanTypesRequestCompleted;
    CompletableFuture<ReferenceRecord> locationsRequestCompleted;
    CompletableFuture<ReferenceRecord> instanceTypesRequestCompleted;
    CompletableFuture<ReferenceRecord> identifierTypesRequestCompleted;
    CompletableFuture<ReferenceRecord> contributorNameTypesRequestCompleted;

    try {
      materialTypesRequestCompleted = wrapWithExceptionHandler(
        routingContext, materialTypesClient.getRecord("Book"));

      loanTypesRequestCompleted = wrapWithExceptionHandler(
        routingContext, loanTypesClient.getRecord("Can Circulate"));

      locationsRequestCompleted = wrapWithExceptionHandler(
        routingContext, locationsClient.getRecord("3rd Floor"));

      instanceTypesRequestCompleted = wrapWithExceptionHandler(
        routingContext, instanceTypesClient.getRecord("Books"));

      identifierTypesRequestCompleted = wrapWithExceptionHandler(
        routingContext, identifierTypesClient.getRecord("ISBN"));

      contributorNameTypesRequestCompleted = wrapWithExceptionHandler(routingContext, contributorNameTypesClient.getRecord("Personal name"));

    } catch (UnsupportedEncodingException e) {
      String error = String.format("Failed to encode query: %s", e.toString());

      log.error(error);
      ServerErrorResponse.internalError(routingContext.response(), error);
      return;
    }

    CompletableFuture.allOf(materialTypesRequestCompleted,
      loanTypesRequestCompleted, locationsRequestCompleted,
      instanceTypesRequestCompleted, identifierTypesRequestCompleted,
      contributorNameTypesRequestCompleted)
      .thenAccept(v -> {
        ReferenceRecord bookMaterialType = materialTypesRequestCompleted.join();
        ReferenceRecord canCirculateLoanType = loanTypesRequestCompleted.join();
        ReferenceRecord mainLibraryLocation = locationsRequestCompleted.join();
        ReferenceRecord booksInstanceType = instanceTypesRequestCompleted.join();
        ReferenceRecord isbnIdentifierType = identifierTypesRequestCompleted.join();
        ReferenceRecord personalContributorNameType = contributorNameTypesRequestCompleted.join();

        if(anyNull(bookMaterialType, canCirculateLoanType, booksInstanceType,
          isbnIdentifierType, personalContributorNameType)) {
          return;
        }

        if(mainLibraryLocation == null) {
          log.warn(
            "Location for ingested records will be null, as could not find main library location");
        }

        String uploadFileName = uploadFileName(routingContext);

        if(StringUtils.isBlank(uploadFileName)) {
          ServerErrorResponse.internalError(routingContext.response(),
            "Unable to get upload file name");

          return;
        }

        wrapWithExceptionHandler(routingContext,
          getFileContents(routingContext.vertx().fileSystem(), uploadFileName))
          .thenAccept(fileContents -> {
            try {
              List<JsonObject> records = new ModsParser(
                new UTF8LiteralCharacterEncoding())
                .parseRecords(fileContents);

              storage.getIngestJobCollection(context)
                .add(new IngestJob(IngestJobState.REQUESTED),
                  success -> {
                    IngestMessages.start(records,
                      singleEntryMap(bookMaterialType),
                      singleEntryMap(canCirculateLoanType),
                      singleEntryMap(mainLibraryLocation),
                      singleEntryMap(isbnIdentifierType),
                      singleEntryMap(booksInstanceType),
                      singleEntryMap(personalContributorNameType),
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
          });
    });
  }

  private void status(RoutingContext routingContext) {
    Context context = new WebContext(routingContext);

    storage.getIngestJobCollection(context)
      .findById(routingContext.request().getParam("id"),
        it -> JsonResponse.success(routingContext.response(),
          new JsonObject().put("status", it.getResult().state.toString())),
        FailureResponseConsumer.serverError(routingContext.response()));
  }

  private Map<String, String> singleEntryMap(ReferenceRecord record) {
    HashMap<String, String> map = new HashMap<>();

    map.put(record.name, record.id);

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

  private <T> CompletableFuture<T> wrapWithExceptionHandler(
    RoutingContext routingContext,
    CompletableFuture<T> future) {

    return future.exceptionally(t -> {
      handleException(routingContext, t);

      return null;
    });
  }

  private void handleException(
    RoutingContext routingContext,
    Throwable exception) {

    log.error(exception);
    ServerErrorResponse.internalError(routingContext.response(),
      exception.toString());
  }

  private boolean anyNull(ReferenceRecord... records) {
    return Stream.of(records).anyMatch(Objects::isNull);
  }

  private CompletableFuture<String> getFileContents(
    io.vertx.core.file.FileSystem fileSystem, String filename) {

    CompletableFuture<String> future = new CompletableFuture<>();

    try {
      fileSystem.readFile(filename,
        result -> {
          if(result.succeeded()) {
            future.complete(result.result().toString());
          }
          else {
            future.completeExceptionally(result.cause());
          }
      });
    }
    catch (Exception e) {
      future.completeExceptionally(e);
    }

    return future;
  }
}
