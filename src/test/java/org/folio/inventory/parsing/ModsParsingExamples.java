package org.folio.inventory.parsing

import io.vertx.core.json.JsonObject
import org.folio.inventory.support.JsonArrayHelper
import org.junit.Test

import java.util.regex.Pattern

class ModsParsingExamples {

  @Test
  void multipleModsRecordCanBeParsedIntoItems() {

    String modsXml = this.getClass()
      .getResourceAsStream('/mods/multiple-example-mods-records.xml')
      .getText("UTF-8")

    List<JsonObject> records = new ModsParser(new UTF8LiteralCharacterEncoding())
      .parseRecords(modsXml);

    assert records.size() == 8

    assert records.stream().allMatch( { it.containsKey("title") } )

    NoTitlesContainEscapedCharacters(records)

    assert records.stream().allMatch( { it.containsKey("identifiers") } )
    assert records.stream().allMatch( { it.getJsonArray("identifiers").size() > 0 } )

    def california = records.find({
      similarTo(it, "California: its gold and its inhabitants", "69228882")
    })

    assert california != null

    assert california.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(california, "UkMaC", "69228882")

    assert california.getJsonArray("creators").size() == 1
    assert hasCreator(california, "Huntley, Henry Veel")

    def studien = records.find({
      similarTo(it, "Studien zur Geschichte der Notenschrift.", "69247446")
    })

    assert studien != null

    assert studien.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(studien, "UkMaC", "69247446")

    assert studien.getJsonArray("creators").size() == 1
    assert hasCreator(studien, "Riemann, Karl Wilhelm J. Hugo.")

    def essays = records.find({
      similarTo(it, "Essays on C.S. Lewis and George MacDonald", "53556908")
    })

    assert essays != null

    assert essays.getJsonArray("identifiers").size() == 3
    assert hasIdentifier(essays, "UkMaC", "53556908")
    assert hasIdentifier(essays, "StGlU", "b13803414")
    assert hasIdentifier(essays, "isbn", "0889464944")

    assert essays.getJsonArray("creators").size() == 1
    assert hasCreator(essays, "Marshall, Cynthia.")

    def sketches = records.find({
      similarTo(it, "Statistical sketches of Upper Canada", "69077747")
    })

    assert sketches != null

    assert sketches.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(sketches, "UkMaC", "69077747")

    assert sketches.getJsonArray("creators").size() == 1
    assert hasCreator(sketches, "Dunlop, William")

    def mcGuire = records.find({
      similarTo(it, "Edward McGuire, RHA", "22169083")
    })

    assert mcGuire != null

    assert hasIdentifier(mcGuire, "isbn", "0716524783")
    assert hasIdentifier(mcGuire, "bnb", "GB9141816")
    assert hasIdentifier(mcGuire, "UkMaC", "22169083")
    assert hasIdentifier(mcGuire, "StEdNL", "1851914")

    assert mcGuire.getJsonArray("creators").size() == 1
    assert hasCreator(mcGuire, "Fallon, Brian.")

    def influenza = records.find({ similarTo(it,
      "Influenza della Poesia sui Costumi", "43620390") })

    assert influenza != null

    assert influenza.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(influenza, "UkMaC", "43620390")

    assert influenza.getJsonArray("creators").size() == 1
    assert hasCreator(influenza, "MABIL, Pier Luigi.")

    def nikitovic = records.find({
      similarTo(it, "Pavle Nik Nikitović", "37696876")
    })

    assert nikitovic != null

    assert nikitovic.getJsonArray("identifiers").size() == 2
    assert hasIdentifier(nikitovic, "UkMaC", "37696876")
    assert hasIdentifier(nikitovic, "isbn", "8683385124")

    assert nikitovic.getJsonArray("creators").size() == 2
    assert hasCreator(nikitovic, "Nikitović, Pavle")
    assert hasCreator(nikitovic, "Božović, Ratko.")

    def grammar = records.find({
      similarTo(it, "Grammaire comparée du grec et du latin", "69250051")
    })

    assert grammar != null

    assert grammar.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(grammar, "UkMaC", "69250051")

    assert grammar.getJsonArray("creators").size() == 2
    assert hasCreator(grammar, "Riemann, Othon.")
    assert hasCreator(grammar, "Goelzer, Henri Jules E.")
  }

  private void NoTitlesContainEscapedCharacters(List<JsonObject> records) {
    assert records.stream().noneMatch({ record ->
      Pattern.compile(
        '(\\\\x[0-9a-fA-F]{2})+',
        Pattern.CASE_INSENSITIVE).matcher(record.getString("title")).find()
    })
  }

  private boolean similarTo(
    JsonObject record,
    String expectedSimilarTitle,
    String expectedBarcode) {

      record.getString("title").contains(expectedSimilarTitle) &&
      record.getString("barcode") == expectedBarcode
  }

  private boolean hasIdentifier(
    JsonObject record,
    String identifierTypeId,
    String value) {

    JsonArrayHelper.toList(
      record.getJsonArray("identifiers")).stream()
      .anyMatch({ it.getString("type") == identifierTypeId && it.getString("value") == value })
  }

  private boolean hasCreator(
    JsonObject record,
    String name) {

    JsonArrayHelper.toList(
      record.getJsonArray("creators")).stream()
      .anyMatch({ it.getString("name") == name })
  }
}


