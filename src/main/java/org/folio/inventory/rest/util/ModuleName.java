package org.folio.inventory.rest.util;

public class ModuleName {
  private static final String MODULE_NAME = "mod_inventory";
  private static final String MODULE_VERSION = "99.99.99";

  private ModuleName() {
  }

  /**
   * The module name with underscore.
   */
  public static String getModuleName() {
    return MODULE_NAME;
  }

  /**
   * The module version taken from pom.xml at compile time.
   */
  public static String getModuleVersion() {
    return MODULE_VERSION;
  }
}
