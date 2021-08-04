package org.folio.inventory.domain.instances.titles;

import java.util.List;

import lombok.Value;

@Value
public class PrecedingSucceedingTitles {

  List<PrecedingSucceedingTitle> precedingSucceedingTitles;
  int totalRecords;

}
