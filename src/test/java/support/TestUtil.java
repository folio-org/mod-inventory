package support;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.folio.HttpStatus;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  @SneakyThrows
  public static String readFileFromPath(String path) {
    return Files.readString(Path.of(path));
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(int httpStatus) {
    return buildHttpResponseWithBuffer(null, httpStatus, "");
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(HttpStatus httpStatus) {
    return buildHttpResponseWithBuffer(null, httpStatus.toInt(), "");
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(HttpStatus httpStatus, String statusMessage) {
    return buildHttpResponseWithBuffer(null, httpStatus.toInt(), statusMessage);
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(Buffer buffer, int httpStatus) {
    return buildHttpResponseWithBuffer(buffer, httpStatus, "");
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(Buffer buffer, HttpStatus httpStatus) {
    return buildHttpResponseWithBuffer(buffer, httpStatus.toInt(), "");
  }

  public static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(Buffer buffer, int httpStatus,
                                                                     String statusMessage) {
    return new HttpResponseImpl<>(null, httpStatus, statusMessage, null, null, null, buffer, null);
  }
}
