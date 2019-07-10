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
    JsonArray contributors) {

    return new JsonObject()
      .put("id",id.toString())
      .put("title", title)
      .put("identifiers", identifiers)
      .put("contributors", contributors)
      .put("source", "Local")
      .put("instanceTypeId", getTextInstanceType());
  }

  public static JsonObject smallAngryPlanet(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "9781473619777"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Chambers, Becky"));

    return createInstanceRequest(id, "Long Way to a Small Angry Planet",
      identifiers, contributors);
  }

  public static JsonObject nod(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01D1PLMDO"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Barnes, Adrian"));

    return createInstanceRequest(id, "Nod", identifiers, contributors);
  }

  public static JsonObject uprooted(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1447294149"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781447294146"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Novik, Naomi"));

    return createInstanceRequest(id, "Uprooted",
      identifiers, contributors);
  }

  public static JsonObject temeraire(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "0007258712"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9780007258710"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Novik, Naomi"));

    return createInstanceRequest(id, "Temeraire",
      identifiers, contributors);
  }

  public static JsonObject leviathanWakes(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1841499897"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781841499895"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Corey, James S. A."));

    return createInstanceRequest(id, "Leviathan Wakes", identifiers, contributors);
  }

  public static JsonObject taoOfPooh(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1405204265"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781405204265"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Hoff, Benjamin"));

    return createInstanceRequest(id, "Tao of Pooh", identifiers, contributors);
  }

  public static JsonObject girlOnTheTrain(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01LO7PJOE"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Hawkins, Paula"));

    return createInstanceRequest(id, "The Girl on the Train",
      identifiers, contributors);
  }

  public static JsonObject treasureIslandInstance(UUID id) {
    return new JsonObject()
      .put("id", id.toString())
      .put("title", "Treasure Island")
      .put("source", "MARC")
      .put("instanceTypeId", getTextInstanceType());
  }

  private static JsonObject identifier(
    String identifierTypeId,
    String value) {

    return new JsonObject()
      .put("identifierTypeId", identifierTypeId)
      .put("value", value);
  }

  private static JsonObject contributor(String name) {
    return new JsonObject()
      .put("contributorNameTypeId", getPersonalContributorNameType())
      .put("name", name);
  }
}
