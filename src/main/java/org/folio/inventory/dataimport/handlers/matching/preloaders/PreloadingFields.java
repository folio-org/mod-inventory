package org.folio.inventory.dataimport.handlers.matching.preloaders;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum PreloadingFields {
    POL("purchaseOrderLineNumber"),
    VRN("vendorReferenceNumber");

    private final String existingMatchField;
}
