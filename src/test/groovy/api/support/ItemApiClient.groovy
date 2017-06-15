package api.support

import io.vertx.core.json.JsonObject
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response
import org.folio.inventory.support.http.client.ResponseHandler

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class ItemApiClient {
  static def createItem(OkapiHttpClient client, JsonObject newItemRequest) {
    def postCompleted = new CompletableFuture<Response>()

    client.post(ApiRoot.items(),
      newItemRequest, ResponseHandler.any(postCompleted))

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assert postResponse.statusCode == 201

    def getCompleted = new CompletableFuture<Response>()

    client.get(postResponse.location, ResponseHandler.json(getCompleted))

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assert getResponse.statusCode == 200

    getResponse.json
  }
}
