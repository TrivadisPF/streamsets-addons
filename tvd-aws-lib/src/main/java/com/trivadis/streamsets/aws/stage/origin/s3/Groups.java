package com.trivadis.streamsets.aws.stage.origin.s3;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  S3("Amazon S3"),
  SSE("SSE"),
  ADVANCED("Advanced"),
  LOOKUP("Lookup"),
  DATA_FORMAT("Data Format"),
  ;

  private final String label;

  Groups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
