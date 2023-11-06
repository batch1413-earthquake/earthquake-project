# Running the back command will automatically create fake data into postgis and send websocet message
.PHONY: back
back:
	poetry run uvicorn earthquake.api.main:app --reload

.PHONY: front
front:
	npm run --prefix earthquake/frontend dev

.PHONY: load-temp-geo-data
load-temp-geo-data:
	ogr2ogr -f "PostgreSQL" PG:"dbname=db user=postgres password=password host=localhost port=5433" earthquake/data/earthquake.geojson -nln earthquakes_data -nlt PROMOTE_TO_MULTI
