package org.folio.inventory.parsing;

import io.vertx.core.json.JsonObject;
import static org.junit.Assert.assertEquals;

import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.support.JsonHelper;
import org.junit.Test;

import java.io.IOException;

// TODO: Extend test data file (or even create multiple)
// TODO: Check if sample data fully represent (all cases of) a true Marc JSON

public class MarcParserTest {

  private MarcParser marcParser;

  {
    try {
      marcParser = new MarcParser();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void marcJson2FolioJson() throws IOException, InvalidMarcJsonException {
    JsonHelper jh = new JsonHelper();
    JsonObject expected = jh.getJsonFileAsJsonObject(
      "/marc/test-output_01a.json");
    JsonObject actual = marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject(
      "/marc/test-input_01a.json"));
    assertEquals(expected.toString(), actual.toString());
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_01.json"));
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_02.json"));
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_03.json"));
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_04.json"));
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_05.json"));
    marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject("/marc/test-entry_06.json"));
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateJsonWithoutFieldsKey() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/marc/has-no-fields.json");
    marcParser.marcJson2FolioJson(jo);
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateJsonFieldsKeyContainsNoArray() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/marc/fields-no-array.json");
    marcParser.marcJson2FolioJson(jo);
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateJsonFieldArrayContainsNonJsonObjectItem() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/marc/non-jsonobject-field.json");
    marcParser.marcJson2FolioJson(jo);
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateJsonFieldArrayContainsEmptyJsonObjectItem() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/marc/empty-jsonobject-field.json");
    marcParser.marcJson2FolioJson(jo);
  }
}
