package org.folio.inventory.rest.util;

public class ModuleName {
  private static final String MODULE_NAME = "mod_inventory";

  private ModuleName() {
  }

  /**
   * The module name with underscore.
   */
  public static String getModuleName() {
    return MODULE_NAME;
  }
}
