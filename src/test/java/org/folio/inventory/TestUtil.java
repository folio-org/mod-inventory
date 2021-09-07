package org.folio.inventory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  public static String readFileFromPath(String path) throws IOException {
    return Files.readString(Path.of(path));
  }
}
