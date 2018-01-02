package org.folio.inventory;

public class Logging {
  private Logging() { }


  static void initialiseFormat() {
    System.setProperty("vertx.logger-delegate-factory-class-name",
      "io.vertx.core.logging.SLF4JLogDelegateFactory");
  }
}
