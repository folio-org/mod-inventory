package api.support

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import static api.ApiTestSuite.*

class InstanceSamples {
  static JsonObject createInstanceRequest(
    UUID id,
    String title,
    JsonArray identifiers,
    JsonArray creators) {

    new JsonObject()
      .put("id",id.toString())
      .put("title", title)
      .put("identifiers", identifiers)
      .put("creators", creators)
      .put("source", "Local")
      .put("instanceTypeId", booksInstanceType)
  }

  static JsonObject smallAngryPlanet(UUID id) {
    def identifiers = new JsonArray()

    identifiers.add(identifier(isbnIdentifierType, "9781473619777"))

    def creators = new JsonArray()

    creators.add(creator("Chambers, Becky"))

    return createInstanceRequest(id, "Long Way to a Small Angry Planet",
      identifiers, creators)
  }

  static JsonObject nod(UUID id) {
    def identifiers = new JsonArray()

    identifiers.add(identifier(asinIdentifierType, "B01D1PLMDO"))

    def creators = new JsonArray()

    creators.add(creator("Barnes, Adrian"))

    createInstanceRequest(id, "Nod", identifiers, creators)
  }

  static JsonObject uprooted(UUID id) {

    def identifiers = new JsonArray();

    identifiers.add(identifier(isbnIdentifierType, "1447294149"));
    identifiers.add(identifier(isbnIdentifierType, "9781447294146"));

    def creators = new JsonArray()

    creators.add(creator("Novik, Naomi"))

    createInstanceRequest(id, "Uprooted",
      identifiers, creators);
  }

  static JsonObject temeraire(UUID id) {

    def identifiers = new JsonArray();

    identifiers.add(identifier(isbnIdentifierType, "0007258712"));
    identifiers.add(identifier(isbnIdentifierType, "9780007258710"));

    def creators = new JsonArray()

    creators.add(creator("Novik, Naomi"))

    createInstanceRequest(id, "Temeraire",
      identifiers, creators);
  }

  static JsonObject leviathanWakes(UUID id) {
    def identifiers = new JsonArray()

    identifiers.add(identifier(isbnIdentifierType, "1841499897"))
    identifiers.add(identifier(isbnIdentifierType, "9781841499895"))

    def creators = new JsonArray()

    creators.add(creator("Corey, James S. A."))

    createInstanceRequest(id, "Leviathan Wakes", identifiers, creators)
  }

  static JsonObject taoOfPooh(UUID id) {
    def identifiers = new JsonArray()

    identifiers.add(identifier(isbnIdentifierType, "1405204265"))
    identifiers.add(identifier(isbnIdentifierType, "9781405204265"))

    def creators = new JsonArray()

    creators.add(creator("Hoff, Benjamin"))

    createInstanceRequest(id, "Tao of Pooh", identifiers, creators)
  }

  static JsonObject girlOnTheTrain(UUID id) {
    def identifiers = new JsonArray()

    identifiers.add(identifier(asinIdentifierType, "B01LO7PJOE"))

    def creators = new JsonArray()

    creators.add(creator("Hawkins, Paula"))

    return createInstanceRequest(id, "The Girl on the Train",
      identifiers, creators)
  }

  private static JsonObject identifier(
    String identifierTypeId,
    String value) {

    return new JsonObject()
      .put("identifierTypeId", identifierTypeId)
      .put("value", value);
  }

  private static JsonObject creator(String name) {
    return new JsonObject()
      .put("creatorTypeId", personalCreatorType)
      .put("name", name);
  }
}
