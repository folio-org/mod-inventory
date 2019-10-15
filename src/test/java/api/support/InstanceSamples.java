package api.support;

import static api.ApiTestSuite.*;

import java.time.LocalDate;
import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.instances.AlternativeTitle;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

public class InstanceSamples {
  public static JsonObject createInstanceRequest(
    UUID id,
    String title,
    JsonArray identifiers,
    JsonArray contributors,
    JsonArray notes) {

    return new JsonObject()
      .put("id",id.toString())
      .put("title", title)
      .put("identifiers", identifiers)
      .put("contributors", contributors)
      .put("notes", notes)
      .put("source", "Local")
      .put("instanceTypeId", getTextInstanceType());
  }

  public static JsonObject smallAngryPlanet(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "9781473619777"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Chambers, Becky"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Small Angry Planet"));

    return createInstanceRequest(id, "Long Way to a Small Angry Planet",
      identifiers, contributors, notes);
  }

  public static JsonObject nod(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01D1PLMDO"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Barnes, Adrian"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Nod"));

    return createInstanceRequest(id, "Nod", identifiers, contributors, notes);
  }

  public static JsonObject uprooted(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1447294149"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781447294146"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Novik, Naomi"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Uprooted"));

    return createInstanceRequest(id, "Uprooted",
      identifiers, contributors, notes);
  }

  public static JsonObject temeraire(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "0007258712"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9780007258710"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Novik, Naomi"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Nod"));

    return createInstanceRequest(id, "Temeraire",
      identifiers, contributors, notes);
  }

  public static JsonObject leviathanWakes(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1841499897"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781841499895"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Corey, James S. A."));
    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Leviathan"));

    return createInstanceRequest(id, "Leviathan Wakes", identifiers, contributors, notes);
  }

  public static JsonObject taoOfPooh(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getIsbnIdentifierType(), "1405204265"));
    identifiers.add(identifier(getIsbnIdentifierType(), "9781405204265"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Hoff, Benjamin"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Tao of Pooh"));

    return createInstanceRequest(id, "Tao of Pooh", identifiers, contributors, notes);
  }

  public static JsonObject girlOnTheTrain(UUID id) {
    JsonArray identifiers = new JsonArray();

    identifiers.add(identifier(getAsinIdentifierType(), "B01LO7PJOE"));

    JsonArray contributors = new JsonArray();

    contributors.add(contributor("Hawkins, Paula"));

    JsonArray notes = new JsonArray();
    notes.add(instanceNote("A general note on Girl on the Train"));


    return createInstanceRequest(id, "The Girl on the Train",
      identifiers, contributors, notes);
  }

  public static JsonObject treasureIslandInstance(UUID id) {
    return new JsonObject()
      .put("id", id.toString())
      .put("title", "Treasure Island")
      .put("source", "MARC")
      .put("instanceTypeId", getTextInstanceType());
  }

  public static JsonObject marcInstanceWithDefaultBlockedFields(UUID id) {
    return new JsonObject()
      .put("id", id.toString())
      // blocked fields
      .put("hrid", UUID.randomUUID().toString())
      .put("alternativeTitles", new JsonArray()
        .add(JsonObject.mapFrom(new AlternativeTitle("test id", "test title"))))
      .put("series", new JsonArray().add("test series"))
      .put("source", "MARC")
      .put("identifiers", new JsonArray()
        .add(JsonObject.mapFrom(new Identifier("test identifier type id", "test identifier value"))))
      .put("contributors", new JsonArray()
        .add(JsonObject.mapFrom(new Contributor("test name type id", "test name", "test type id", "test text", true))))
      .put("publication", new JsonArray()
        .add(JsonObject.mapFrom(new Publication("test publisher", "test place", LocalDate.now().toString(), "test role"))))
      .put("editions", new JsonArray().add("test edition"))
      .put("physicalDescriptions", new JsonArray().add("test physical description"))
      .put("instanceFormatIds", new JsonArray().add("test format ids"))
      .put("languages", new JsonArray().add("test language"))
      .put("publicationFrequency", new JsonArray().add("test publication frequency"))
      .put("publicationRange", new JsonArray().add("test publication range"))
      .put("notes", new JsonArray()
        .add(JsonObject.mapFrom(new JsonObject().put("note", "test note").put("staffOnly", false))))
      .put("electronicAccess", new JsonArray()
        .add(JsonObject.mapFrom(new ElectronicAccess("test uri", "test link text", "test materials specification", "test public note", "test relationship id"))))
      .put("subjects", new JsonArray().add("test subject"))
      .put("classifications", new JsonArray()
        .add(JsonObject.mapFrom(new Classification("test id", "test number"))))
      .put("catalogedDate", LocalDate.now().toString())
      .put("title", "Treasure Island")
      .put("indexTitle", "Index Title")
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

  private static JsonObject instanceNote(String note) {
    return new JsonObject()
      .put("instanceNoteTypeId", "6a2533a7-4de2-4e64-8466-074c2fa9308c" )
      .put("note", note);
  }
}
