package org.folio.inventory.domain;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.items.CQLQueryRequestDto;

import java.io.UnsupportedEncodingException;
import java.util.function.Consumer;

public interface SearchableCollection<T> {
  void findByCql(
    String cqlQuery, PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultsCallback,
    Consumer<Failure> failureCallback) throws UnsupportedEncodingException;

  void retrieveByCqlBody(
          CQLQueryRequestDto cqlQueryRequestDto,
          Consumer<Success<MultipleRecords<T>>> resultCallback,
          Consumer<Failure> failureCallback);
}
