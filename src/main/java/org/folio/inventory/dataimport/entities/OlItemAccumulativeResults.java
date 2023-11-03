package org.folio.inventory.dataimport.entities;

import lombok.Getter;
import lombok.Setter;
import org.folio.inventory.domain.items.Item;

import java.util.ArrayList;
import java.util.List;

/**
 * Interface for storing intermediate results of the Item(errors and successful) Handler's processing between runs if Optimistic Locking reveals.
 * It is needed for provided intermediate results between runs via DataImportEventPayload  by key "OL_ACCUMULATIVE_RESULTS".
 * It avoids using class-level fields.
 */
@Getter
@Setter
public class OlItemAccumulativeResults{

  private List<Item> resultedSuccessItems;
  private List<PartialError> resultedErrorItems;

  public OlItemAccumulativeResults() {
    resultedSuccessItems = new ArrayList<>();
    resultedErrorItems = new ArrayList<>();
  }

  /**
   * Clear all lists.
   */
  public void cleanup() {
    resultedSuccessItems.clear();
    resultedErrorItems.clear();
  }
}
