package org.folio.inventory.dataimport.util;

import org.folio.dataimport.util.marc.MarcContentHolder;
import org.folio.dataimport.util.marc.MarcRecordEditor;

/**
 * {@link MarcContentHolder} adapter over {@code org.folio.rest.jaxrs.model.Record} (the FOLIO record type used by
 * {@link AdditionalFieldsUtil}), so {@link MarcRecordEditor} can operate on it without depending on that type
 * directly.
 */
public class JaxrsRecordHolder implements MarcContentHolder {

  private final org.folio.rest.jaxrs.model.Record record;

  public JaxrsRecordHolder(org.folio.rest.jaxrs.model.Record record) {
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
    // matches AdditionalFieldsUtil's pre-extraction getRecordId(Record): "" (not null) for a null record, since
    // log call sites format this into messages and a null-vs-empty-string difference would show up in log output.
    return record != null ? record.getId() : "";
  }
}
