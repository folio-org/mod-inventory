package org.folio.inventory.dataimport.entities;

import lombok.Getter;
import lombok.Setter;
import org.folio.HoldingsRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for storing intermediate results of the Holdings(errors and successful) Handler's processing between runs if Optimistic Locking reveals.
 * It is needed for provided intermediate results between runs via DataImportEventPayload  by key "OL_ACCUMULATIVE_RESULTS".
 * It avoids using class-level fields.
 */
@Getter
@Setter
public class OlHoldingsAccumulativeResults {

  private List<HoldingsRecord> resultedSuccessHoldings;
  private List<PartialError> resultedErrorHoldings;

  public OlHoldingsAccumulativeResults() {
    resultedSuccessHoldings = new ArrayList<>();
    resultedErrorHoldings = new ArrayList<>();
  }

  /**
   * Clear all lists.
   */
  public void cleanup() {
    resultedSuccessHoldings.clear();
    resultedErrorHoldings.clear();
  }
}
