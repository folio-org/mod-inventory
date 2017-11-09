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


    def studien = records.find({
      similarTo(it, "Studien zur Geschichte der Notenschrift.", "69247446")
    })

    assert studien != null

    assert studien.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(studien, "UkMaC", "69247446")

    def essays = records.find({
      similarTo(it, "Essays on C.S. Lewis and George MacDonald", "53556908")
    })

    assert essays != null

    assert essays.getJsonArray("identifiers").size() == 3
    assert hasIdentifier(essays, "UkMaC", "53556908")
    assert hasIdentifier(essays, "StGlU", "b13803414")
    assert hasIdentifier(essays, "isbn", "0889464944")


    def sketches = records.find({
      similarTo(it, "Statistical sketches of Upper Canada", "69077747")
    })

    assert sketches != null

    assert sketches.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(sketches, "UkMaC", "69077747")

    def mcGuire = records.find({
      similarTo(it, "Edward McGuire, RHA", "22169083")
    })

    assert mcGuire != null

    assert hasIdentifier(mcGuire, "isbn", "0716524783")
    assert hasIdentifier(mcGuire, "bnb", "GB9141816")
    assert hasIdentifier(mcGuire, "UkMaC", "22169083")
    assert hasIdentifier(mcGuire, "StEdNL", "1851914")

    def influenza = records.find({ similarTo(it,
      "Influenza della Poesia sui Costumi", "43620390") })

    assert influenza != null

    assert influenza.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(influenza, "UkMaC", "43620390")

    def nikitovic = records.find({
      similarTo(it, "Pavle Nik Nikitović", "37696876")
    })

    assert nikitovic != null

    assert nikitovic.getJsonArray("identifiers").size() == 2
    assert hasIdentifier(nikitovic, "UkMaC", "37696876")
    assert hasIdentifier(nikitovic, "isbn", "8683385124")

    def grammar = records.find({
      similarTo(it, "Grammaire comparée du grec et du latin", "69250051")
    })

    assert grammar != null

    assert grammar.getJsonArray("identifiers").size() == 1
    assert hasIdentifier(grammar, "UkMaC", "69250051")
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
    String namespace,
    String value) {

    JsonArrayHelper.toList(
      record.getJsonArray("identifiers")).stream()
      .anyMatch({ it.getString("namespace") == namespace && it.getString("value") == value })
  }
}


