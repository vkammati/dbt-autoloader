<div style="position:relative; margin-top:1rem; margin-bottom:1rem; heigh: 130px; display:flex; flex-direction:row;
    align-items:center;">
    <img src="./misc/logo.svg" width="64px" height="64px" style="margin:1rem;"/>
    <h1>Template EDP Auto Loader</h1>
</div>

<div style="position:relative; display:flex; flex-direction:row; column-gap: .5rem; align-items:center;">
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=alert_status&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=sqale_rating&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=reliability_rating&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=security_rating&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=vulnerabilities&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
    <img src="https://sesvc.shell.com/api/project_badges/measure?project=com.shell.Template-EDP-Auto-Loader&metric=coverage&token=sqb_62343603cfa26fe8d9a496115d847a8bd75f64af"/>
</div>

<br/>

<a name="readme-top"></a>

# Welcome to the Auto loader reference pipeline on EDP Databricks!

This project implements a Python package for using Databricks Auto Loader in the Enterprise Data Platform (EDP).

It can be used to load data from the 'landing' layer (in all sorts of formats and schemas) into the 'raw' layer in Delta Lake format and Unity Catalog.

All the configuration is done using a YAML file. This project implements best practices with respect to security, logging and deployment.

## We, the [DBT CoE](https://github.com/orgs/sede-x/teams/pde-office), are maintaining and improving this reference pipeline daily.

To be informed of our last releases, subscribe to our teams channel [Auto loader QnA](https://teams.microsoft.com/l/channel/19%3A1a660109eacd4755ae0def74daeb250b%40thread.tacv2/DBT%20QnA?groupId=210eca16-716e-41e7-b567-990fd80f3981&tenantId=db1e96a8-a3da-442a-930b-235cac24cd5c)

Please find our complete documentation in [DALPS](https://dalps.shell.com/technologies/edpl/auto-loader/General/)

![Screenshot 2024-05-23 at 16 58 03](https://github.com/sede-x/Template-EDP-Auto-Loader/assets/8851473/609c2e9d-cab9-456e-8844-a2c6a990616a)

### What is in the scope of this project?
- Batch load
- Streaming load
- Job scheduler
- Multiple environments (dev, tst, pdr...)
- CI/CD automation
- Azure integration
- Databricks integration
- Git - Code version control
- Git action - Automation
- Terraform - infrastructure as code
- SonarQube integration
- **IRM approval**

## Get started with a new project [here](https://dalps.shell.com/technologies/edpl/auto-loader/Get-started/)
Pre requirement
- Github repository. Get a template from us.
- Terraform cloud
- Azure
  - Storage account
  - Service principal (SPN)
- Databricks Workspace
- SonarQube

## Maintainers
Please contact the [DBT CoE](https://github.com/orgs/sede-x/teams/pde-office) if you have any questions about this repository.
Email: siti-gx-gl-ida-dbt-coe@shell.com
Principal engineer: Sertac Oruc

For more information on Auto Loader: [Microsoft learn](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
For more information about EDP: [DALPS](https://dalps.shell.com/technologies/edpl/summary/summary/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>
