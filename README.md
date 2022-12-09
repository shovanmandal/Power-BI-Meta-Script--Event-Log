# pbi_meta
Python application to export Power BI metadata and log data for use by Enterprise Data

Data is sourced from the Power [BI API interface](https://docs.microsoft.com/en-us/rest/api/power-bi/).

---

Primarily the two endpoints are used:

* Workspace Metadata - [get-modified-workspaces](https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-modified-workspaces)
* Power BI Logs - [get-activity-events](https://docs.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events)
