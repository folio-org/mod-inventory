package org.folio.inventory.dataimport.util;

import org.folio.processing.events.utils.PomReaderUtil;

public final class ConsumerWrapperUtil {

  private ConsumerWrapperUtil() {
  }

  public static String constructModuleName() {
    return PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(PomReaderUtil.INSTANCE.getModuleName(), PomReaderUtil.INSTANCE.getVersion());
  }
}
