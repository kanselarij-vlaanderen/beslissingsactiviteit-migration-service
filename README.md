# Beslissingsactiviteit migration

## Add the beslissingsactiviteit migration service service to your stack temporarily

Add the following snippet to your `docker-compose.override.yml`:
```yml
  beslissingsactiviteit-migration-service:
  build: https://github.com/kanselarij-vlaanderen/beslissingsactiviteit-migration-service.git
  environment:
    NODE_ENV: "development"
  links:
    - triplestore:database
```

The service is finished when the log says 
* `Finished migrating agenda-item-treatments on agendaitem`