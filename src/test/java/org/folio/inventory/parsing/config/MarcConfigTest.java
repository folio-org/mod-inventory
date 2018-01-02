package org.folio.inventory.parsing.config;

import org.folio.inventory.exceptions.InvalidMarcConfigException;
import org.junit.Test;

public class MarcConfigTest {

  @Test(expected = InvalidMarcConfigException.class)
  public void validateConfigNoKeyMarcFields() throws Exception {
    new MarcConfig(
      "config/config-no-key-marc-fields.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateConfigNoKeyInstanceFields() throws Exception {
    new MarcConfig(
      "config/config-no-key-instance-fields.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateConfigInstanceFieldsNoArray() throws Exception {
    new MarcConfig(
      "config/config-instance-fields-no-array.json");
  }
  @Test(expected = InvalidMarcConfigException.class)
  public void validateConfigNoJsonObjectInArray() throws Exception {
    new MarcConfig(
      "config/config-no-jsonobject-in-array.json");
  }
  @Test
  public void validateValid() throws Exception {
    new MarcConfig(
      "config/marc-config-valid.json");
  }
}
