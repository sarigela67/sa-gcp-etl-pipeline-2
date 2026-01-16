"""Enterprise ETL infrastructure for sonic-earth-476701-m1 project."""

import pulumi
import pulumi_gcp as gcp

project_id = "sonic-earth-476701-m1"
region = "us-central1"
location = "US"

# Curated data bucket
curated_bucket = gcp.storage.Bucket("simple-de-curated",
    name="sonic-earth-476701-m1-simple-de-curated",
    location=location,
    storage_class="STANDARD",
    uniform_bucket_level_access=True,
    force_destroy=True,  # Learning only; remove in prod
    project=project_id,
    labels={
        "environment": "learning",
        "team": "simple-de",
        "purpose": "curated-data"
    })

# Code bucket for PySpark jobs
code_bucket = gcp.storage.Bucket("simple-de-code",
    name="sonic-earth-476701-m1-simple-de-code",
    location=location,
    storage_class="STANDARD",
    uniform_bucket_level_access=True,
    force_destroy=True,
    project=project_id,
    labels={
        "environment": "learning",
        "team": "simple-de",
        "purpose": "pyspark-jobs"
    })

# Small Dataproc cluster for ETL jobs (ephemeral pattern)
dataproc_cluster = gcp.dataproc.Cluster("simple-de-cluster",
    project=project_id,
    region=region,
    cluster_config=gcp.dataproc.ClusterClusterConfigArgs(
        # Master node
        master_config=gcp.dataproc.ClusterClusterConfigMasterConfigArgs(
            num_instances=1,
            machine_type="e2-standard-2",
        ),
        # Worker nodes
        worker_config=gcp.dataproc.ClusterClusterConfigWorkerConfigArgs(
            num_instances=2,
            machine_type="e2-standard-2",
        ),
        # Software config
        software_config=gcp.dataproc.ClusterClusterConfigSoftwareConfigArgs(
            image_version="2.2-debian12",
            optional_components=["JUPYTER"],
        ),
        # GCS config for staging
        gce_cluster_config=gcp.dataproc.ClusterClusterConfigGceClusterConfigArgs(
            zone="us-central1-a",
        )
    ),
    labels={
        "environment": "learning",
        "team": "simple-de",
    })

# Export key names
pulumi.export("project_id", project_id)
pulumi.export("curated_bucket_name", curated_bucket.name)
pulumi.export("code_bucket_name", code_bucket.name)
pulumi.export("dataproc_cluster_name", dataproc_cluster.cluster_name)
pulumi.export("dataproc_region", region)
