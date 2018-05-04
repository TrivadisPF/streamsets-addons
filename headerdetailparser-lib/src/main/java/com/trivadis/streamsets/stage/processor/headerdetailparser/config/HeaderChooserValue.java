package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class HeaderChooserValue extends BaseEnumChooserValues<HeaderType> {

  public HeaderChooserValue() {
    super(HeaderType.class);
  }

}
