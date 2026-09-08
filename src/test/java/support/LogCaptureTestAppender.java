package support;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;

/**
 * Test-only log4j2 appender that captures formatted log messages emitted by a given class's logger, so tests
 * can assert on log output (level, message content) without depending on a mocking framework's static-mocking
 * of {@code LogManager}. Attach at the start of a test, detach in a {@code finally} block once assertions are
 * done, so the capture doesn't leak into other tests.
 */
public final class LogCaptureTestAppender extends AbstractAppender {

  private final List<String> messages = new CopyOnWriteArrayList<>();
  private final org.apache.logging.log4j.core.Logger targetLogger;

  private LogCaptureTestAppender(org.apache.logging.log4j.core.Logger targetLogger) {
    super(LogCaptureTestAppender.class.getSimpleName() + "@" + System.identityHashCode(targetLogger), null, null);
    this.targetLogger = targetLogger;
  }

  /**
   * Attaches a new capturing appender to the logger named after {@code loggedClass} (matching classes that
   * obtain their logger via {@code LogManager.getLogger()} with no arguments).
   *
   * @param loggedClass class whose logger should be captured
   * @return the attached, started appender; call {@link #detach()} once done with it
   */
  public static LogCaptureTestAppender attachTo(Class<?> loggedClass) {
    var logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(loggedClass.getName());
    var appender = new LogCaptureTestAppender(logger);
    ((LoggerContext) LogManager.getContext(false)).getConfiguration().addAppender(appender);
    logger.addAppender(appender);
    appender.start();
    return appender;
  }

  @Override
  public void append(LogEvent event) {
    messages.add(event.getMessage().getFormattedMessage());
  }

  public List<String> getMessages() {
    return messages;
  }

  /**
   * Stops this appender and detaches it from the logger it was attached to.
   */
  public void detach() {
    stop();
    targetLogger.removeAppender(this);
  }
}
