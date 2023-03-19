# SFCN_MangroveMarshReport.py
Script to Summarize Mangrove Marsh Monitoring data including output tables and figures for annual reporting.

## Script performs the following routines:
 1) Summaries the Event Average Distance from Ground Truth values.  This includes Averge, Standard Error, Confidence Interval for defined %, and Maximum and Minimum values by event/segment replicating the 'Mangrove-Marsh Ecotone Monitoring' SOP8-1 summary table. Confidence Intervals are defined using a Student T Distribution with Student T defiend as: np.abs(t.ppf((1 - confidence) / 2, dof)).

2) Summarize via a CrossTab/Pivot Table the Absolute Vegetation by Region, Location Name (i.e. Point on Segment), by Community Type, and by Taxon (Scale is Point - single value). Routine replicates the 'Mangrove-Marsh Ecotone Monitoring' SOP8-2 summary table.

3) Calculates the Absolute Cover By Region, By Community, By Strata - data is from table  'tbl_MarkerData'. Output replicates the 'Mangrove-Marsh Ecotone Monitoring' SOP8-3 summary Figure.

## Output:
An excel spreadsheet with Tables SOP8-1 (i.e. Average Distance from Ground Truth values), Tables SOP8-2 By region (three total) with the the Absolute Vegetation by Region, Location Name (i.e. Point on Segment), by Community Type, and by Taxon, and a pdf file withe Figures SOP8-3 by region (three total) with the Absolute Cover By Region, By Community, By Strata.
