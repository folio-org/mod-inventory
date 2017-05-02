package api.support

import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import org.folio.inventory.common.testing.HttpClient

class ControlledVocabularyPreparation {
  private final HttpClient client
  private final URL controlledVocabularyRoot
  private final String collectionWrapperProperty

  ControlledVocabularyPreparation(
    HttpClient client,
    URL controlledVocabularyRoot,
    String collectionWrapperProperty) {

    this.controlledVocabularyRoot = controlledVocabularyRoot
    this.client = client
    this.collectionWrapperProperty = collectionWrapperProperty
  }

  String createOrReference(String name) {
    def (getResponse, wrappedEntries) = client.get(controlledVocabularyRoot)

    if(getResponse.status != 200) {
      println("Controlled vocabulary API unavailable")
      assert false
    }

    def existingEntries = wrappedEntries[this.collectionWrapperProperty]

    if (existingEntries.stream().noneMatch({ it.name == name })) {
      def vocabularyEntryRequest = new JsonObject().put("name", name);

      def (postResponse, createdMaterialType) = client.post(
        controlledVocabularyRoot, Json.encodePrettily(vocabularyEntryRequest))

      assert postResponse.status == 201

      createdMaterialType.id
    } else {
      existingEntries.stream()
        .filter({ it.name == name })
        .findFirst().get().id
    }
  }
}
