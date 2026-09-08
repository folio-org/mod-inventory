package org.folio.inventory.domain.instances.titles;

import java.util.List;

public record PrecedingSucceedingTitleCollection(
  List<PrecedingSucceedingTitle> precedingSucceedingTitles,
  int totalRecords) { }
