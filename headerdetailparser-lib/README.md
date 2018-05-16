# Header/Detail Parser
The Header/Detail Parser Processor parses a text file which contains first of some header lines followed by detail lines. The detail lines can either be sparated by a special separator line or their can be a fixed number of header lines. 
By specifying the header line number and a regular expression the values of the header can be extracted as columns. 
The detail line can either be passed as is, to be processed outside of the Header Details Parser Processor or it can be split by specifying a separator.

The detail lines can also contain an optional detail column header (the first line after the header), which can be used as names when splitting the detail line.

For splitting a detail line, the Header/Detail Parser Processor contains some of the functionality of the Field Splitter Processor, which is part of the base SDC deployment. 

When you configure the Field Splitter, you specify the field to split, the regular expression to use as a separator, and the fields to use for split data. You configure what to do when a record does not have the expected number of splits, and what to do when a record has additional data with more possible splits.

You can keep the original field being split or discard it.

## Configuring a Header/Detail Parser
Configure a Header/Detail Parser to parse data from a single field with a header and details into structured lines. 

1) In the Properties panel, on the General tab, configure the following properties:

| General Property | Description  | 
|:---------------- |:-------------|
| Name | Stage name. |
| Description | Optional description. |
| Required Fields | Fields that must include data for the record to be passed into the stage.<br/><br/>**Tip**: You might include fields that the stage uses. <br/><br/>Records that do not include all required fields are processed based on the error handling configured for the pipeline. |
| Preconditions | Conditions that must evaluate to TRUE to allow a record to enter the stage for processing. Click Add to create additional preconditions.<br><br>Records that do not meet all preconditions are processed based on the error handling configured for the stage.|
| On Record Error | Error record handling for the stage:<br><br><li>Discard - Discards the record.<li>Send to Error - Sends the record to the pipeline for error handling.<li>Stop Pipeline - Stops the pipeline. Not valid for cluster pipelines.|

2) On the **Parser** tab, configure the following properties:

| Parser Property | Description | 
|:---------------- |:-------------|
| Input Data Format | The Data format for the input to the processor. Use one of the following formats:<li>Text<li>Text (Blob)<li>Whole File |
| Keep Original Fields | Optional description. |
| Required Fields | xxxx. |
| Output Field | xxxx |
| Detail Line Field | xxx |
| Split Details? | xxx |

3) On the **Header** tab, configure the following properties:

| Header Property | Description | 
|:---------------- |:-------------|
| Header Extractors |  |
| Header/Detail Separator | A regular expression identifying the line which separates the header from the details section. |
| Number of Header Lines | The number lines which are part of the header. |

4) On the **Details** tab, configure the following properties:

| Details Property | Description | 
|:---------------- |:-------------|
| Column Header Line | Indicates whether the details data contains a colum header line, and whether to use the header line.<li>Use Header Line - There is a header line and it should be used to name the columns.<li>Ignore Header Line - There is a header line but it should not be used to name the columns. Use the New Split Fields property to name the columns.<li>No Header Line - There is no detail header line.|
| Separator| The regular expression to use to split data in a field. For some tips on using regular expressions, see [Regular Expressions Overview](https://streamsets.com/documentation/datacollector/latest/help/#datacollector/UserGuide/Apx-RegEx/RegEx-Title.html#concept_vd4_nsc_gs). |
| New Split Fields | Names of the new fields to pass the split data.<br><br>**Note:** Precede each field name with a slash as follows: `/NewField`. |
| Not Enough Splits | Record handling when the data does not include as many splits as the specified number of split fields:<li>Continue - Passes the record split as much as possible with null values in unused split fields.<li>Send to Error - Sends the record to the pipeline for error handling.|
| Too Many Splits | Record handling when the data contains more potential splits than the specified number of split fields:<br><li>Put Remaining Text in Last Field - Writes any additional data to the last split field.<li>Store Remaining Splits as List - Splits the additional data and writes the splits to the specified List field.|
| Field for Remaining Splits | List field for remaining splits. Used when the data includes more splits than expected by the processor. |
