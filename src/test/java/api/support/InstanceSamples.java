package api.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

import static api.ApiTestSuite.*;

public class InstanceSamples {
  public static JsonObject createInstanceRequest(
    UUID id,
    String title,
    JsonArray identifiers,
    JsonArray creators) {

    return new JsonObject()
      .put("id",id.toString())
      .put("title", title)
      .put("identifiers", identifiers)
      .put("creators", creators)
      .put("source", "Local")
      .put("instanceTypeId", getBooksInstanceType());
  }

  public static JsonObject smallAngryPlanet(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "9781473619777"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Chambers, Becky"));

    return createInstanceRequest(id, "Long Way to a Small Angry Planet",
      identifiers, creators);
  }

  public static JsonObject nod(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01D1PLMDO"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Barnes, Adrian"));

    return createInstanceRequest(id, "Nod", identifiers, creators);
  }

  public static JsonObject uprooted(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1447294149"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781447294146"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Novik, Naomi"));

    return createInstanceRequest(id, "Uprooted",
      identifiers, creators);
  }

  public static JsonObject temeraire(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "0007258712"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9780007258710"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Novik, Naomi"));

    return createInstanceRequest(id, "Temeraire",
      identifiers, creators);
  }

  public static JsonObject leviathanWakes(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1841499897"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781841499895"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Corey, James S. A."));

    return createInstanceRequest(id, "Leviathan Wakes", identifiers, creators);
  }

  public static JsonObject taoOfPooh(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1405204265"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781405204265"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Hoff, Benjamin"));

    return createInstanceRequest(id, "Tao of Pooh", identifiers, creators);
  }

  public static JsonObject girlOnTheTrain(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01LO7PJOE"));

    JsonArray creators = new JsonArray();

    creators.add(creator("Hawkins, Paula"));

    return createInstanceRequest(id, "The Girl on the Train",
      identifiers, creators);
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
      .put("creatorTypeId", getPersonalCreatorType())
      .put("name", name);
  }
}
