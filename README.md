
# TuKano app ported into Azure

## Azure resources setup
From AzureTools folder:
- run `mvn clean compile assembly:single`
- run `java -cp target/scc2425-mgt-1.0-jar-with-dependencies.jar scc.mgt.AzureManagement`
- Change names in `azureprops-<region>.sh` from `fun<your_id><region>` to `app<your_id><region>` 
- run `./azureprops-<region>.sh`

#### Deleting Azure resources
From AzureTools folder
- run `java -cp target/scc2425-mgt-1.0-jar-with-dependencies.jar scc.mgt.AzureManagement --delete`

## Setup project locally:
From scc-2425-tukano folder:
- run `mvn clean install exec:java`

## Deploy project on Azure:
From scc-2425-tukano folder:
- run `mvn clean compile package azure-webapp:deploy`

## Azure functions setup
From AzureFunctions folder:
- run `mvn clean compile package azure-functions:deploy
`