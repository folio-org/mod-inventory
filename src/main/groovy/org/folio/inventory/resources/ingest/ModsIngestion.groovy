package org.folio.inventory.resources.ingest

import io.vertx.groovy.ext.web.Router
import io.vertx.groovy.ext.web.RoutingContext
import io.vertx.groovy.ext.web.handler.BodyHandler
import org.folio.inventory.CollectionResourceClient
import org.folio.inventory.common.WebContext
import org.folio.inventory.common.api.response.ClientErrorResponse
import org.folio.inventory.common.api.response.FailureResponseConsumer
import org.folio.inventory.common.api.response.JsonResponse
import org.folio.inventory.common.api.response.RedirectResponse
import org.folio.inventory.common.api.response.ServerErrorResponse
import org.folio.inventory.common.domain.Success
import org.folio.inventory.domain.ingest.IngestMessages
import org.folio.inventory.parsing.ModsParser
import org.folio.inventory.parsing.UTF8LiteralCharacterEncoding
import org.folio.inventory.storage.Storage
import org.folio.inventory.support.JsonArrayHelper
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response

import java.util.concurrent.CompletableFuture

class ModsIngestion {
  private final Storage storage

  ModsIngestion(final Storage storage) {
    this.storage = storage
  }

  public void register(Router router) {
    router.post(relativeModsIngestPath() + "*").handler(BodyHandler.create())
    router.post(relativeModsIngestPath()).handler(this.&ingest)
    router.get(relativeModsIngestPath() + "/status/:id").handler(this.&status)
  }

  private ingest(RoutingContext routingContext) {
    if(routingContext.fileUploads().size() > 1) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Cannot parse multiple files in a single request")
      return
    }

    //TODO: Will only work for book material type and can circulate loan type
    def context = new WebContext(routingContext)

    def client = new OkapiHttpClient(routingContext.vertx().createHttpClient(),
      new URL(context.okapiLocation), context.tenantId,
      context.token,
      { ServerErrorResponse.internalError(routingContext.response(),
      "Failed to retrieve material types: ${it}")})

    def materialTypesClient = new CollectionResourceClient(client,
      new URL(context.okapiLocation + "/material-types"))

    def loanTypesClient = new CollectionResourceClient(client,
      new URL(context.okapiLocation + "/loan-types"))

    def materialTypesRequestCompleted = new CompletableFuture<Response>()
    def loanTypesRequestCompleted = new CompletableFuture<Response>()

    materialTypesClient.getMany(
      "query=" + URLEncoder.encode("name=\"Book\"", "UTF-8"),
      { response -> materialTypesRequestCompleted.complete(response) })

    loanTypesClient.getMany(
      "query=" + URLEncoder.encode("name=\"Can Circulate\"", "UTF-8"),
      { response -> loanTypesRequestCompleted.complete(response) })

    CompletableFuture.allOf(materialTypesRequestCompleted, loanTypesRequestCompleted)
      .thenAccept({ v ->

      def materialTypeResponse = materialTypesRequestCompleted.get()
      def loanTypeResponse = loanTypesRequestCompleted.get()

      if (materialTypeResponse.statusCode != 200) {
        ServerErrorResponse.internalError(routingContext.response(),
          "Unable to retrieve material types: ${materialTypeResponse.statusCode}: ${materialTypeResponse.body}")

        return
      }

      if (loanTypeResponse.statusCode != 200) {
        ServerErrorResponse.internalError(routingContext.response(),
          "Unable to retrieve loan types: ${loanTypeResponse.statusCode}: ${loanTypeResponse.body}")

        return
      }

      def bookMaterialTypeId =
        JsonArrayHelper.toList(materialTypeResponse.json.getJsonArray("mtypes"))
          .first().getString("id")

      def canCirculateLoanTypeId =
        JsonArrayHelper.toList(loanTypeResponse.json.getJsonArray("loantypes"))
          .first().getString("id")

      routingContext.vertx().fileSystem().readFile(uploadFileName(routingContext),
        { result ->
          if (result.succeeded()) {
            def uploadedFileContents = result.result().toString()

            def records = new ModsParser(new UTF8LiteralCharacterEncoding())
              .parseRecords(uploadedFileContents)

            def convertedRecords = new IngestRecordConverter().toJson(records)

            storage.getIngestJobCollection(context)
              .add(new IngestJob(IngestJobState.REQUESTED),
              { Success success ->

                IngestMessages.start(convertedRecords,
                  ["Book": bookMaterialTypeId],
                  ["Can Circulate": canCirculateLoanTypeId],
                  success.result.id, context)
                  .send(routingContext.vertx())

                RedirectResponse.accepted(routingContext.response(),
                  statusLocation(routingContext, success.result.id))
              },
              {
                println("Creating Ingest Job failed")
              })

          } else {
            ServerErrorResponse.internalError(
              routingContext.response(), result.cause().toString())
          }
        })
    })
  }

  private status(RoutingContext routingContext) {

    def context = new WebContext(routingContext)

    storage.getIngestJobCollection(context)
      .findById(routingContext.request().getParam("id"),
      { Success it ->
        JsonResponse.success(routingContext.response(),
          ["status" : it.result.state.toString()])
      }, FailureResponseConsumer.serverError(routingContext.response()))
  }

  private String statusLocation(RoutingContext routingContext, jobId) {

    def scheme = routingContext.request().scheme()
    def host = routingContext.request().host()

    "${scheme}://${host}${relativeModsIngestPath()}/status/${jobId}"
  }

  private String uploadFileName(RoutingContext routingContext) {
    routingContext.fileUploads().toList().first()
      .uploadedFileName()
  }

  private static String relativeModsIngestPath() {
    "/inventory/ingest/mods"
  }
}
