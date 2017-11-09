package org.folio.inventory.common.domain;

import java.util.List;

public class  MultipleRecords <T> {
  public final List<T> records;
  public final Integer totalRecords;

  public MultipleRecords(List<T> records, Integer totalRecords) {
    this.records = records;
    this.totalRecords = totalRecords;
  }
}
