package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class HeaderChooserValue extends BaseEnumChooserValues<DetailsColumnHeaderType> {

  public HeaderChooserValue() {
    super(DetailsColumnHeaderType.class);
  }

}
