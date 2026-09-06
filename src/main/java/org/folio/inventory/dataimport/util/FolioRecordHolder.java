package org.folio.inventory.dataimport.util;

/**
 * {@link MarcContentHolder} adapter over {@code org.folio.Record} (the jsonschema2pojo-generated FOLIO record type
 * used by {@code MarcRecordUtil}), so {@link MarcRecordEditor} can operate on it without depending on that type
 * directly.
 */
public class FolioRecordHolder implements MarcContentHolder {

  private final org.folio.Record record;

  public FolioRecordHolder(org.folio.Record record) {
    this.record = record;
  }

  @Override
  public Object getMarcContent() {
    return record != null && record.getParsedRecord() != null ? record.getParsedRecord().getContent() : null;
  }

  @Override
  public void setMarcContent(String content) {
    record.setParsedRecord(record.getParsedRecord().withContent(content));
  }

  @Override
  public String getRecordId() {
    // mirrors JaxrsRecordHolder.getRecordId(): "" (not null) for a null record, for the same log-formatting reason.
    return record != null ? record.getId() : "";
  }
}
