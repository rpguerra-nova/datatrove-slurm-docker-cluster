# Datatrove Slurm Docker Cluster

This is an implementation of a Datatrove processing pipeline running with Slurm inside a 
Docker container. Some modifications were made to the original Slurm Docker Cluster setup 
to allow the Datatrove environment to run. We also elected to use a single compute node for 
Slurm.

**IMPORTANT**: A working `datatrove_env.tar.gz`, containing a conda-packed environment with 
all the libraries required for the Datatrove processing pipeline is REQUIRED to be placed in
`slurm-docker-cluster/app/` for this project to function. The conda environment, named 
`datatrove_env`, should be based in Python 3.11, with [datatrove-amalia](https://github.com/AMALIA-NOVA/datatrove-amalia) 
installed, along with its requirements.

In `.github/workflows/ci.yml` is the workflow to run this project, which also shows the steps 
needed for manual execution. It contains some important variables to be set, either in this file 
or manually exported into `env`, depending on the machine running the container and the data to 
be processed, which are:

- **SLURM_HOST_CPUS**: Number of CPUs to use in Slurm's compute node.
- **SLURM_HOST_MEMORY**: Amount of memory available to use in Slurm's compute  node, in MB.
- **HOST_INPUT_PATH**: The path to the collection containing WARC files to be processed in the host machine.
- **HOST_OUTPUT_PATH**: The path where the processing outputs will be written in the host machine.
- **HOST_LOGS_PATH**: The path where the processing logs will be written in the host machine.
- **DATATROVE_TASKS**: Number of total tasks to run in the scraping/filtering portion of the Datatrove pipeline. This is divided by 5 for the deduplication portion.
- **DATATROVE_WORKERS**: Number of workers to concurrently run tasks in the Datatrove pipeline.
- **DATATROVE_CPUS_PER_TASK**: Number of CPUs to be used per task in the Datatrove pipeline.
- **DATATROVE_MEM_PER_CPU_GB**: Memory used per task in the Datatrove pipeline, in GB.

The original README for Slurm Docker Cluster is shown below.

# Slurm Docker Cluster

**Slurm Docker Cluster** is a multi-container Slurm cluster designed for rapid
deployment using Docker Compose. This repository simplifies the process of
setting up a robust Slurm environment for development, testing, or lightweight
usage.

## 🏁 Getting Started

To get up and running with Slurm in Docker, make sure you have the following tools installed:

- **[Docker](https://docs.docker.com/get-docker/)**
- **[Docker Compose](https://docs.docker.com/compose/install/)**

Clone the repository:

```bash
git clone https://github.com/giovtorres/slurm-docker-cluster.git
cd slurm-docker-cluster
```

## 📦 Containers and Volumes

This setup consists of the following containers:

- **mysql**: Stores job and cluster data.
- **slurmdbd**: Manages the Slurm database.
- **slurmctld**: The Slurm controller responsible for job and resource management.
- **c1**: Compute node (running `slurmd`).

### Persistent Volumes:

- `etc_munge`: Mounted to `/etc/munge`
- `etc_slurm`: Mounted to `/etc/slurm`
- `slurm_jobdir`: Mounted to `/data`
- `var_lib_mysql`: Mounted to `/var/lib/mysql`
- `var_log_slurm`: Mounted to `/var/log/slurm`

## 🛠️  Building the Docker Image

The version of the Slurm project and the Docker build process can be simplified
by using a `.env` file, which will be automatically picked up by Docker Compose.

Update the `SLURM_TAG` and `IMAGE_TAG` found in the `.env` file and build
the image:

```bash
docker compose build
```

Alternatively, you can build the Slurm Docker image locally by specifying the
[SLURM_TAG](https://github.com/SchedMD/slurm/tags) as a build argument and
tagging the container with a version ***(IMAGE_TAG)***:

```bash
docker build --build-arg SLURM_TAG="slurm-21-08-6-1" -t slurm-docker-cluster:21.08.6 .
```

## 🚀 Starting the Cluster

Once the image is built, deploy the cluster with the default version of slurm
using Docker Compose:

```bash
docker compose up -d
```

To specify a specific version and override what is configured in `.env`, specify
the `IMAGE_TAG`:

```bash
IMAGE_TAG=21.08.6 docker compose up -d
```

This will start up all containers in detached mode. You can monitor their status using:

```bash
docker compose ps
```

## 📝 Register the Cluster

After the containers are up and running, register the cluster with **SlurmDBD**:

```bash
./register_cluster.sh
```

> **Tip**: Wait a few seconds for the daemons to initialize before running the registration script to avoid connection errors like:
> `sacctmgr: error: Problem talking to the database: Connection refused`.

For real-time cluster logs, use:

```bash
docker compose logs -f
```

## 🖥️  Accessing the Cluster

To interact with the Slurm controller, open a shell inside the `slurmctld` container:

```bash
docker exec -it slurmctld bash
```

Now you can run any Slurm command from inside the container:

```bash
[root@slurmctld /]# sinfo
PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
normal*      up 5-00:00:00      2   idle c[1-2]
```

## 🧑‍💻 Submitting Jobs

The cluster mounts the `slurm_jobdir` volume across all nodes, making job files accessible from the `/data` directory. To submit a job:

```bash
[root@slurmctld /]# cd /data/
[root@slurmctld data]# sbatch --wrap="hostname"
Submitted batch job 2
```

Check the output of the job:

```bash
[root@slurmctld data]# cat slurm-2.out
c1
```

## 🔄 Cluster Management

### Stopping and Restarting:

Stop the cluster without removing the containers:

```bash
docker compose stop
```

Restart it later:

```bash
docker compose start
```

### Deleting the Cluster:

To completely remove the containers and associated volumes:

```bash
docker compose down -v
```

## ⚙️ Advanced Configuration

You can modify Slurm configurations (`slurm.conf`, `slurmdbd.conf`) on the fly without rebuilding the containers. Just run:

```bash
./update_slurmfiles.sh slurm.conf slurmdbd.conf
docker compose restart
```

This makes it easy to add/remove nodes or test new configuration settings dynamically.

## 🤝 Contributing

Contributions are welcomed from the community! If you want to add features, fix bugs, or improve documentation:

1. Fork this repo.
2. Create a new branch: `git checkout -b feature/your-feature`.
3. Submit a pull request.

## 📄 License

This project is licensed under the [MIT License](LICENSE).
