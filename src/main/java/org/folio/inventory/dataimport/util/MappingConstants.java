package org.folio.inventory.dataimport.util;

import static org.folio.inventory.domain.instances.Instance.INSTANCE_TYPE_ID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.inventory.domain.instances.Instance.TITLE_KEY;

import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class MappingConstants {

  public static final String MARC_BIB_RECORD_TYPE = "marc-bib";
  public static final String MARC_BIB_RECORD_FORMAT = "MARC_BIB";
  public static final String INSTANCE_PATH = "instance";
  public static final List<String> INSTANCE_REQUIRED_FIELDS = Arrays.asList(SOURCE_KEY, TITLE_KEY, INSTANCE_TYPE_ID_KEY);

}
