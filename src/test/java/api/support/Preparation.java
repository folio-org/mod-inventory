package api.support

import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response
import org.folio.inventory.support.http.client.ResponseHandler

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class Preparation {
  private final OkapiHttpClient client

  Preparation(OkapiHttpClient client) {
    this.client = client
  }

  void deleteInstances() {
    deleteAll(ApiRoot.instances())
  }

  void deleteItems() {
    deleteAll(ApiRoot.items())
  }

  private void deleteAll(URL root) {
    def getCompleted = new CompletableFuture<Response>()

    client.delete(root,
      ResponseHandler.any(getCompleted))

    Response response = getCompleted.get(5, TimeUnit.SECONDS);

    assert response.statusCode == 204
  }
}
