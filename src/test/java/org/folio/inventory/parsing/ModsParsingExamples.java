package org.folio.inventory.parsing;

import com.google.common.io.CharStreams;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.JsonArrayHelper;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ModsParsingExamples {

  @Test
  public void multipleModsRecordCanBeParsedIntoItems()
    throws ParserConfigurationException,
    SAXException,
    XPathExpressionException,
    IOException {

    String modsXml;

    try (final Reader reader = new InputStreamReader(this.getClass()
      .getResourceAsStream("/mods/multiple-example-mods-records.xml"), "UTF-8")) {
      modsXml = CharStreams.toString(reader);
    }

    List<JsonObject> records = new ModsParser(new UTF8LiteralCharacterEncoding())
      .parseRecords(modsXml);

    assertThat(records.size(), is(9));

    assertThat(records.stream().allMatch(it -> it.containsKey("title")), is(true));

    NoTitlesContainEscapedCharacters(records);

    assertThat(records.stream().allMatch(it -> it.containsKey("identifiers")), is(true));
    assertThat(records.stream().allMatch(it -> it.getJsonArray("identifiers").size() > 0), is(true));

    JsonObject california = getRecord(records,
      "California: its gold and its inhabitants", "69228882");

    assertThat(california, is(notNullValue()));

    assertThat(california.getJsonArray("identifiers").size(), is(1));
    assertThat(hasIdentifier(california, "UkMaC", "69228882"), is(true));

    assertThat(california.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(california, "Huntley, Henry Veel"), is(true));

    JsonObject studien = getRecord(records,
      "Studien zur Geschichte der Notenschrift.", "69247446");

    assertThat(studien, is(notNullValue()));

    assertThat(studien.getJsonArray("identifiers").size(), is(1));
    assertThat(hasIdentifier(studien, "UkMaC", "69247446"), is(true));

    assertThat(studien.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(studien, "Riemann, Karl Wilhelm J. Hugo."), is(true));

    JsonObject essays = getRecord(records,
      "Essays on C.S. Lewis and George MacDonald", "53556908");

    assertThat(essays, is(notNullValue()));

    assertThat(essays.getJsonArray("identifiers").size(), is(3));
    assertThat(hasIdentifier(essays, "UkMaC", "53556908"), is(true));
    assertThat(hasIdentifier(essays, "StGlU", "b13803414"), is(true));
    assertThat(hasIdentifier(essays, "isbn", "0889464944"), is(true));

    assertThat(essays.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(essays, "Marshall, Cynthia."), is(true));

    JsonObject sketches = getRecord(records,
      "Statistical sketches of Upper Canada", "69077747");

    assertThat(sketches, is(notNullValue()));

    assertThat(sketches.getJsonArray("identifiers").size(), is(1));
    assertThat(hasIdentifier(sketches, "UkMaC", "69077747"), is(true));

    assertThat(sketches.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(sketches, "Dunlop, William"), is(true));

    JsonObject mcGuire = getRecord(records, "Edward McGuire, RHA", "22169083");

    assertThat(mcGuire, is(notNullValue()));

    assertThat(hasIdentifier(mcGuire, "isbn", "0716524783"), is(true));
    assertThat(hasIdentifier(mcGuire, "bnb", "GB9141816"), is(true));
    assertThat(hasIdentifier(mcGuire, "UkMaC", "22169083"), is(true));
    assertThat(hasIdentifier(mcGuire, "StEdNL", "1851914"), is(true));

    assertThat(mcGuire.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(mcGuire, "Fallon, Brian."), is(true));

    JsonObject influenza = getRecord(records,
      "Influenza della Poesia sui Costumi", "43620390");

    assertThat(influenza, is(notNullValue()));

    assertThat(influenza.getJsonArray("identifiers").size(), is(1));
    assertThat(hasIdentifier(influenza, "UkMaC", "43620390"), is(true));

    assertThat(influenza.getJsonArray("contributors").size(), is(1));
    assertThat(hasContributor(influenza, "MABIL, Pier Luigi."), is(true));

    JsonObject nikitovic = getRecord(records, "Pavle Nik Nikitović", "37696876");

    assertThat(nikitovic, is(notNullValue()));

    assertThat(nikitovic.getJsonArray("identifiers").size(), is(2));
    assertThat(hasIdentifier(nikitovic, "UkMaC", "37696876"), is(true));
    assertThat(hasIdentifier(nikitovic, "isbn", "8683385124"), is(true));

    assertThat(nikitovic.getJsonArray("contributors").size(), is(2));
    assertThat(hasContributor(nikitovic, "Nikitović, Pavle"), is(true));
    assertThat(hasContributor(nikitovic, "Božović, Ratko."), is(true));

    JsonObject grammar = getRecord(records,
      "Grammaire comparée du grec et du latin", "69250051");

    assertThat(grammar, is(notNullValue()));

    assertThat(grammar.getJsonArray("identifiers").size(), is(1));
    assertThat(hasIdentifier(grammar, "UkMaC", "69250051"), is(true));

    assertThat(grammar.getJsonArray("contributors").size(), is(2));
    assertThat(hasContributor(grammar, "Riemann, Othon."), is(true));
    assertThat(hasContributor(grammar, "Goelzer, Henri Jules E."), is(true));

    JsonObject smallAngry = getRecord(records,
      "The Long Way to a Small, Angry Planet", "67437645");

    assertThat(smallAngry, is(notNullValue()));

    assertThat(smallAngry.getJsonArray("identifiers").size(), is(3));
    assertThat(hasIdentifier(smallAngry, "UkMaC", "684566576"), is(true));
    assertThat(hasIdentifier(smallAngry, "isbn", "1473619793"), is(true));
    assertThat(hasIdentifier(smallAngry, "isbn", "9781473619791"), is(true));

    assertThat(smallAngry.getJsonArray("contributors").size(), is(0));

  }

  @Test
  public void createsIdentifierWithBlankTypeWhenAttributeNotPresent()
    throws ParserConfigurationException,
    SAXException,
    XPathExpressionException,
    IOException {

    String modsXml;

    try (final Reader reader = new InputStreamReader(this.getClass()
      .getResourceAsStream("/mods/no-identifier-source-or-type.xml"), "UTF-8")) {
      modsXml = CharStreams.toString(reader);
    }

    List<JsonObject> records = new ModsParser(new UTF8LiteralCharacterEncoding())
      .parseRecords(modsXml);

    assertThat(records.size(), is(2));

    JsonObject firstRecord = records.get(0);
    List<JsonObject> firstRecordIdentifiers = JsonArrayHelper.toList(
      firstRecord.getJsonArray("identifiers"));

    assertThat(firstRecordIdentifiers.size(), is(1));
    assertThat(firstRecordIdentifiers.get(0).getString("type"), is(""));
    assertThat(firstRecordIdentifiers.get(0).getString("value"), is("no-source"));

    JsonObject secondRecord = records.get(1);
    List<JsonObject> secondRecordIdentifiers = JsonArrayHelper.toList(
      secondRecord.getJsonArray("identifiers"));

    assertThat(secondRecordIdentifiers.size(), is(1));
    assertThat(secondRecordIdentifiers.get(0).getString("type"), is(""));
    assertThat(secondRecordIdentifiers.get(0).getString("value"), is("no-type"));
  }

  private static JsonObject getRecord(
    List<JsonObject> records,
    String title,
    String barcode) {

    return records.stream()
      .filter(it -> similarTo(it, title, barcode))
      .findFirst()
      .orElse(null);
  }

  private static void NoTitlesContainEscapedCharacters(List<JsonObject> records) {
    assertThat(records.stream().noneMatch(record ->
      Pattern.compile(
        "(\\\\x[0-9a-fA-F]{2})+",
        Pattern.CASE_INSENSITIVE).matcher(record.getString("title")).find()),
      is(true));
  }

  private static boolean similarTo(
    JsonObject record,
    String expectedSimilarTitle,
    String expectedBarcode) {

      return record.getString("title").contains(expectedSimilarTitle) &&
        StringUtils.equals(record.getString("barcode"), expectedBarcode);
  }

  private static boolean hasIdentifier(
    JsonObject record,
    String identifierTypeId,
    String value) {

    return JsonArrayHelper.toList(
      record.getJsonArray("identifiers")).stream()
      .anyMatch(it -> StringUtils.equals(it.getString("type"), identifierTypeId)
        && StringUtils.equals(it.getString("value"), value));
  }

  private static boolean hasContributor(
    JsonObject record,
    String name) {

    return JsonArrayHelper.toList(
      record.getJsonArray("contributors")).stream()
      .anyMatch(it -> StringUtils.equals(it.getString("name"), name));
  }
}


