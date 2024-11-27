
# TuKano app ported into Kubernetes

## Creating .war file:
From scc-2425-tukano folder:
- run `mvn clean install`

## Run docker-compose:
From scc-2425-tukano folder:
- run `docker-compose up --build`
### Endpoints should be available at the following URLs:
- For shorts: http://localhost:8080/tukano-1/rest/shorts
- For users: http://localhost:8080/tukano-1/rest/users

## Stopping Docker Containers
#### Stopping docker containers:
- run `docker-compose down`
#### Stopping docker containers and removing volumes:
- run `docker-compose down -v`