# Misc sheet

## Creating Postgres Table SQL
```sql
CREATE TABLE "tbldata" (
  "id" serial NOT NULL,
  PRIMARY KEY ("id"),
  "city_id" integer NOT NULL,
  "lon" real NOT NULL,
  "lat" real NOT NULL,
  "temp" real NOT NULL,
  "feels_like" real NOT NULL,
  "city_name" character(255) NOT NULL,
  "country" character(8) NOT NULL,
  "timezone" integer NOT NULL,
  "date" timestamp UNIQUE NOT NULL
)
```
## Docker commands
```bash
docker-compose -f stack.yml up
docker-compose -f stack.yml down

docker volume ls
docker volmue inspect [VOLUME_NAME]
docker volume prune -f -a (removes all volumes by force)
```


## EXPORT CSV FROM DOCKER POSTGRES CONTAINER
```bash
docker exec -it -u database_user_name container_name \
psql -d database_name -c "COPY (SELECT * FROM table) TO STDOUT WITH CSV HEADER" > output.csv
```
```bash
docker exec -it -u postgres etl_weather_db psql -c "COPY (SELECT * FROM tblweatherdata LIMIT 5) TO STDOUT WITH CSV HEADER;" -d openweather > tblweatherdata.csv
```