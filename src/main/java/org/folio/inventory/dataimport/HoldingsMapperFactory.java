package org.folio.inventory.dataimport;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.processing.mapping.mapper.Mapper;
import org.folio.processing.mapping.mapper.mappers.HoldingsMapper;
import org.folio.processing.mapping.mapper.mappers.MapperFactory;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ProfileType;

import java.util.LinkedHashMap;

public class HoldingsMapperFactory implements MapperFactory {

  public static final String EXISTING_RECORD_TYPE = "existingRecordType";

  @Override
  public Mapper createMapper(Reader reader, Writer writer) {
    return new HoldingsMapper(reader, writer);
  }

  @Override
  public boolean isEligiblePayload(DataImportEventPayload eventPayload) {
    LinkedHashMap<String, String> map = (LinkedHashMap<String, String>) eventPayload.getCurrentNode().getContent();
    String existingRecordType = map.get(EXISTING_RECORD_TYPE);
    return (ProfileType.MAPPING_PROFILE.equals(eventPayload.getCurrentNode().getContentType()))
      && (existingRecordType.equals(ActionProfile.FolioRecord.HOLDINGS.value()));
  }
}
