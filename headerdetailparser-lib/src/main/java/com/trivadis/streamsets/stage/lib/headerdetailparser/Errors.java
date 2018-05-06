package com.trivadis.streamsets.stage.lib.headerdetailparser;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
	
  HEADERDETAILP_00("Define at least one new split fields"),
  HEADERDETAILP_01("Unsupported field type '{}' with value '{}' encountered in record '{}'"),
  HEADERDETAILP_02("The detail line does not have enough splits"),
  HEADERDETAILP_03("Field Path at index '{}' cannot be empty"),
  HEADERDETAILP_04("IOException attempting to parse whole file for record '{}': {}"),
  HEADERDETAILP_05("Empty detail line '{}': {}"),
  HEADERDETAILP_06("Empty blob"),
  HEADERDETAILP_07("Cannot set field '{}' for record '{}', reason : {}")
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}