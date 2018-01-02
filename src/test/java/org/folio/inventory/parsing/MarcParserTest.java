package org.folio.inventory.parsing;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import static org.junit.Assert.assertEquals;

import org.folio.inventory.exceptions.InvalidMarcConfigException;
import org.folio.inventory.exceptions.InvalidMarcJsonException;
import org.folio.inventory.exceptions.MarcParseException;
import org.folio.inventory.support.JsonHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;

// TODO: Extend test data file (or even create multiple)
// TODO: Check if sample data fully represent (all cases of) a true Marc JSON

@RunWith(JUnitParamsRunner.class)
public class MarcParserTest {
  /**
   * Read the resource and convert the file to JsonObject.
   * @param resourcePath  where to read
   * @return JsonObject
   */
  private JsonObject read(String resourcePath) {
    try {
      return JsonHelper.getJsonFileAsJsonObject(resourcePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parse the MARC file at the resourcePath.
   * @param resourcePath  file to parse
   * @return result folio json data
   * @throws MarcParseException
   */
  private JsonObject parse(String resourcePath) throws MarcParseException  {
    return new MarcParser().marcJson2FolioJson(read(resourcePath));
  }

  /**
   * Parse the MARC file at the resourcePath.
   * @param configPath  config file to use
   * @param resourcePath  file to parse
   * @return result folio json data
   * @throws InvalidMarcJsonException on parse error
   */
  private JsonObject parse(String configPath, String resourcePath) throws MarcParseException  {
    try {
      return new MarcParser(configPath).marcJson2FolioJson(read(resourcePath));
    } catch (InvalidMarcConfigException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Parameters({
    "marc/00.json, marc/00-out.json",
  })
  public void parseDefaultConfig(String input, String expected) throws MarcParseException {
    assertEquals(read(expected).toString(), parse(input).toString());
  }

  @Test
  @Parameters({
    "config/marc-config-valid.json, marc/02.json, marc/02-out-short.json",
  })
  public void parseSpecialConfig(String config, String input, String expected) throws MarcParseException {
    assertEquals(read(expected).toString(), parse(config, input).toString());
  }

  @Test
  @Parameters({
    "marc/01.json",
    "marc/02.json",
    "marc/03.json",
    "marc/04.json",
    "marc/05.json",
    "marc/06.json",
  })
  public void parseDefaultConfig2(String resourcePath) throws MarcParseException {
    parse(resourcePath);
  }

  @Test(expected = InvalidMarcJsonException.class)
  @Parameters({
    "marc/has-no-fields.json",
    "marc/fields-no-array.json",
    "marc/non-jsonobject-field.json",
    "marc/empty-jsonobject-field.json",
    "marc/has-no-fields.json",
    "marc/fields-no-array.json",
    "marc/empty-jsonobject-field.json",
  })
  public void parseThrowsInvalidMarcJsonException(String resourcePath) throws MarcParseException {
    parse(resourcePath);
  }
}
