package org.folio.inventory.support;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

public class JsonHelperTest {
  @Test
  public void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(JsonHelper.class);
  }

  @Test(expected = FileNotFoundException.class)
  public void throwsFileNotFound() throws IOException {
    JsonHelper.getJsonFileAsJsonObject("/path/that/not/exists");
  }
}
