package org.folio.inventory.parsing;

import io.vertx.core.json.JsonObject;
import static org.junit.Assert.assertEquals;

import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.support.JsonHelper;
import org.junit.Test;
import java.io.*;

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
      "/sample-data/marc-json/test-output_01a.json");
    JsonObject actual = marcParser.marcJson2FolioJson(jh.getJsonFileAsJsonObject(
      "/sample-data/marc-json/test-input_01a.json"));
    assertEquals(expected.toString(), actual.toString());
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateInvalidJson1() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/sample-data/marc-json/has-no-fields.json");
    marcParser.marcJson2FolioJson(jo);
  }

  @Test(expected = InvalidMarcJsonException.class)
  public void validateInvalidJson2() throws IOException, InvalidMarcJsonException {
    JsonObject jo = new JsonHelper().getJsonFileAsJsonObject("/sample-data/marc-json/fields-no-array.json");
    marcParser.marcJson2FolioJson(jo);
  }
}
