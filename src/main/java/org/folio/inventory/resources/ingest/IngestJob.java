package org.folio.inventory.resources.ingest;

public class IngestJob {
  public final String id;
  public final IngestJobState state;

  public IngestJob(String id, IngestJobState state) {
    this.id = id;
    this.state = state;
  }

  public IngestJob(IngestJobState state) {
    this(null, state);
  }

  public IngestJob complete() {
    return new IngestJob(this.id, IngestJobState.COMPLETED);
  }

  public IngestJob copyWithNewId(String newId) {
    return new IngestJob(newId, this.state);
  }
}
