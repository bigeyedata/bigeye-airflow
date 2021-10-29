# bigeye-airflow
Airflow operators to interact with Bigeye.

### How to Test
[Astronomer](astronomer.io) provides a local runtime for Airflow DAGs.  This runtime
is build from the Dockerfile in the astro folder.  A startup bash script has been 
added, ***astro_dev***, to facilitate the needed copies into the astro environment.
```shell
bash astro_dev start
bash astro_dev stop
bash astro_dev restart
```

