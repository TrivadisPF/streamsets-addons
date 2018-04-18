package com.trivadis.streamsets.utahparser.stage.lib.sample;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  UTAHP_00("JSON field '{}' does not exist in record '{}'. Cannot parse the field."),
  UTAHP_01("Cannot parse"),
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