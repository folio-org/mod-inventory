package org.folio.inventory.rest.util;

import static java.lang.String.format;

public class ModuleUtil {

  private static final String MODULE_NAME = ModuleName.getModuleName();

  /**
   * RMB convention driven tenant to schema name
   *
   * @param tenantId
   * @return formatted schema and module name
   */
  public static String convertToPsqlStandard(String tenantId){
    return format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

}
