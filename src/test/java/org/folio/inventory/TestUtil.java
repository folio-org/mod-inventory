package org.folio.inventory;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  public static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }
}
