# personal_finance

If another postgresql instance may be running, check using the following command, after logging in as super user.

lsof -i :5432

This will list the instanes running. If  another instance is running it can be ended by

kill [pid]

To launch the postgresql server via docker.
docker run --rm   --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data  postgres

To package:
mvn package
mvn clean compile assembly:single
java -jar target/personal_finance-1.0-SNAPSHOT-jar-with-dependencies.jar


to log into psql
psql -h localhost -U postgres -d postgres

