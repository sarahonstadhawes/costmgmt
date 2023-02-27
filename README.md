# costmgmt: 
Cost Analysis Tool 
- Azure data factory pipelines that collect and transform cost data: 
Check_Refined_Folder pipeline checks if a refined file has been made on the current day in storage; if not run the other pipeline     PL_RUN_DBX_NB) which checks if a raw file has been made for current day; if it has run databricks notebook that creates refined file for the day - Databricks Notebook creates a file daily that contains the current day cost information as well as previous months report
- PowerBi Report shows an example of cost data from file created in databricks
- ARM template of the data factory pipelines 
