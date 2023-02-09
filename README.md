# costmgmt
Azure data factory pipeline that collects and transforms cost data 
- (Check_Refined_Folder) pipeline checks if a refined file has been made on the current day in storage; if not run the other pipeline
- (PL_RUN_DBX_NB) pipeline checks if a raw file has been made for current day; if it has run databricks notebook that creates refined file for the day (contains current day cost information as well as previous months)
