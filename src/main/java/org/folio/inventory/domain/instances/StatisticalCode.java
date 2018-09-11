/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class StatisticalCode {
  // JSON property names
  public static final String STATISTICAL_CODE_TYPE_ID_KEY = "statisticalCodeTypeId";
  public static final String CODE_KEY = "code";
  
  public final String statisticalCodeTypeId;
  public final String code;
  
  public StatisticalCode(String statisticalCodeTypeId, String code) {
    this.statisticalCodeTypeId = statisticalCodeTypeId;
    this.code = code;
  }

  public StatisticalCode(JsonObject json) {
        this(json.getString(STATISTICAL_CODE_TYPE_ID_KEY),
         json.getString(CODE_KEY));
  }
  
}
