package api.support

import io.vertx.core.json.JsonObject
import org.folio.inventory.support.JsonArrayHelper
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.folio.inventory.support.http.client.Response
import org.folio.inventory.support.http.client.ResponseHandler

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class ControlledVocabularyPreparation {
  private final OkapiHttpClient client
  private final URL controlledVocabularyRoot
  private final String collectionWrapperProperty

  ControlledVocabularyPreparation(
    OkapiHttpClient client,
    URL controlledVocabularyRoot,
    String collectionWrapperProperty) {

    this.controlledVocabularyRoot = controlledVocabularyRoot
    this.client = client
    this.collectionWrapperProperty = collectionWrapperProperty
  }

  String createOrReferenceTerm(String name) {

    def getCompleted = new CompletableFuture<Response>()

    client.get(controlledVocabularyRoot,
      ResponseHandler.json(getCompleted))

    Response response = getCompleted.get(5, TimeUnit.SECONDS);

    if(response.statusCode != 200) {
      println("Controlled vocabulary API unavailable")
      assert false
    }

    def existingTerms = JsonArrayHelper.toList(response.json.getJsonArray(this.collectionWrapperProperty))

    if (existingTerms.stream().noneMatch({ it.getString(name).equals(name) })) {
      def vocabularyEntryRequest = new JsonObject().put("name", name);

      def postCompleted = new CompletableFuture<Response>()

      client.post(controlledVocabularyRoot,
        vocabularyEntryRequest, ResponseHandler.json(postCompleted))

      Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

      assert postResponse.statusCode == 201

      postResponse.json.getString("id")
    } else {
      existingTerms.stream()
        .filter({ it.getString(name).equals(name) })
        .findFirst().get().getString("id")
    }
  }
}
