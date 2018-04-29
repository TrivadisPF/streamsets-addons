package com.trivadis.streamsets.stage.lib.headerdetailparser;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  HEADERDETAILP_00("JSON field '{}' does not exist in record '{}'. Cannot parse the field."),
  HEADERDETAILP_01("Cannot parse"),
  HEADERDETAILP_02("Unsupported field type '{}' with value '{}' encountered in record '{}'"),

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