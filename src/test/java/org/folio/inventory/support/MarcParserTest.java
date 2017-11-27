package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.Instance;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import java.io.*;
import java.util.ArrayList;

// TODO: Extend test data file (or even create multiple)
// TODO: Check if sample data fully represent (all cases of) a true Marc JSON

public class MarcParserTest {

  private MarcParser marcParser = new MarcParser();
  private JsonObject marcEntry;
  private Instance testInstance = this.createTestInstance();

  @Before
  public void setUp() throws Exception {
    this.importMarcJsonSampleData();
  }

  @After
  public void tearDown() throws Exception {
    marcEntry.clear();
  }

  @Test
  public void marcJsonToFolioInstance() throws Exception {
    Instance instance = marcParser.marcJsonToFolioInstance(marcEntry);
    assertEquals(testInstance.id, instance.id);
    assertEquals(testInstance.title, instance.title);
    assertEquals(testInstance.source, instance.source);
    assertEquals(testInstance.instanceTypeId, instance.instanceTypeId);
    assertEquals(testInstance.identifiers, instance.identifiers);
    assertEquals(testInstance.creators, instance.creators);
  }

  private void importMarcJsonSampleData() {
    this.marcEntry = new JsonObject(this.getJsonString());
  }

  private String getJsonString() {
    String file = "/sample-data/marc-json/test-entry_01.json";
    InputStream is = this.getClass().getResourceAsStream(file);
    String jsonString =  null;
    try {
      jsonString = this.readFile(is);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return jsonString;
  }

  private String readFile(InputStream is) throws IOException {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      StringBuilder sb = new StringBuilder();
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append("\n");
        line = br.readLine();
      }
      return sb.toString();
    }
  }

  private Instance createTestInstance() {
    return new Instance(
      "",
      "title remainder_of_title state_of_responsibility inclusive_dates bulk_dates medium",
      new ArrayList<>(),
      "",
      "",
      new ArrayList<>());
  }
}
