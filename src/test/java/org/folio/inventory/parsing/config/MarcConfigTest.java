package org.folio.inventory.parsing.config;

import org.folio.inventory.exceptions.InvalidMarcConfigException;
import org.junit.Test;

public class MarcConfigTest {

  @Test(expected = InvalidMarcConfigException.class)
  public void validateInvalid1() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-invalid_1.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateInvalid2() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-invalid_2.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateInvalid3() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-invalid_3.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateInvalid4() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-invalid_4.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateInvalid5() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-invalid_5.json");
  }
  @Test
  public void validateValid() throws Exception {
    new MarcConfig(
      "/sample-data/marc-config/marc-config-valid.json");
  }
}
