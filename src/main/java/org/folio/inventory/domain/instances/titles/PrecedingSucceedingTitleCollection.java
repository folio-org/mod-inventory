package org.folio.inventory.domain.instances.titles;

import java.util.List;

public class PrecedingSucceedingTitleCollection {

  private final List<PrecedingSucceedingTitle> precedingSucceedingTitles;
  private final int totalRecords;

  public PrecedingSucceedingTitleCollection(List<PrecedingSucceedingTitle> precedingSucceedingTitles, int totalRecords) {
    this.precedingSucceedingTitles = precedingSucceedingTitles;
    this.totalRecords = totalRecords;
  }

  public List<PrecedingSucceedingTitle> getPrecedingSucceedingTitles() {
    return precedingSucceedingTitles;
  }

  public int getTotalRecords() {
    return totalRecords;
  }
}
