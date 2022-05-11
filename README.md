# bigeye-airflow
Airflow operators to interact with Bigeye API.

## The Test Environment
Setup/Initialization
```shell
bash quickstart.sh -i
```
Run
```shell
bash quickstart.sh
```

### Credentials Setup
* Go to the Admin menu and choose Connections:
    ![Admin Menu - Connections](docs/images/astronomer_connections_1.png "Admin Menu - Connections")  

* Fill out the form for an HTTP connection using the appropriate Host, User and Pass. The current test Dags use 
'bigeye_connection' as a connection_id for basic auth to the Bigeye API.
    ![Admin Menu - Connections](docs/images/astronomer_connections_2.png "Admin Menu - Connections") 

### Testing Metric Creation:  
Testing in a DAG runtime can be achieved by altering the test dag: 
[test_create_metrics_dag.py](https://github.com/bigeyedata/bigeye-airflow/blob/main/astro/dags/test_create_metrics_dag.py)
