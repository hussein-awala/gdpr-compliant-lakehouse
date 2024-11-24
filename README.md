# How to create a GDPR-compliant Iceberg Lakehouse

This repository is a demonstration of how to handle GDPR export and delete requests in an Iceberg Lakehouse to make it
GDPR-compliant.

## Run the demo

To run the demo, you need to have Docker and Docker Compose installed on your machine. Then, you can run the following
command:

```bash
docker-compose up -d
```

This command will start a Docker container with a Jupyter notebook server, a container with minio (a S3-compatible
object storage) and a container with an Iceberg REST catalog.

In your browser, navigate to [http://localhost:8888/lab/tree/work/notebooks/GDPR-demo.ipynb](http://localhost:8888/lab/tree/work/notebooks/GDPR-demo.ipynb) and run the notebook.

## Shut down the demo

To shut down the demo, you can run the following command:

```bash
docker-compose down --volumes
```

This command will stop and remove the Docker containers and delete the data volumes, but it will keep all the notebooks
and data files created in the `work` directory.