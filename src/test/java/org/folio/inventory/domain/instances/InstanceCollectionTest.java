package org.folio.inventory.domain.instances;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.common.domain.Success;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the {@link InstanceCollection#findByCql(String, boolean, PagingParameters, Consumer,
 * Consumer)} default method: implementations with no notion of shadow copies inherit a default
 * that ignores {@code includeShadowCopies} and delegates to the plain CQL search.
 */
class InstanceCollectionTest {

  @ParameterizedTest(name = "includeShadowCopies={0}")
  @ValueSource(booleans = {true, false})
  @DisplayName("should delegate to the plain CQL search regardless of includeShadowCopies")
  void shouldDelegateToPlainCqlSearch_regardlessOfIncludeShadowCopies(boolean includeShadowCopies)
    throws Exception {

    // arrange
    InstanceCollection collection = mock(InstanceCollection.class, CALLS_REAL_METHODS);
    String cqlQuery = "title=\"*Angry*\"";
    PagingParameters pagingParameters = PagingParameters.defaults();
    Consumer<Success<MultipleRecords<Instance>>> resultCallback = success -> { };
    Consumer<Failure> failureCallback = failure -> { };

    // act
    collection.findByCql(cqlQuery, includeShadowCopies, pagingParameters, resultCallback, failureCallback);

    // assert
    verify(collection).findByCql(eq(cqlQuery), eq(pagingParameters), any(), any());
  }
}
