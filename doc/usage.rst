Running at NERSC
----------------

A local copy of this package can be downloaded from github:

.. code-block:: bash

   $ git clone https://github.com/LSSTDESC/bps_parsl_sites.git

To set up it up at NERSC, it's probably easiest to use the CVMFS distributions of the Rubin stack.  From a perlmutter login node (running bash), one can do

.. code-block:: bash

   $ source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2025_43/loadLSST.bash
   (lsst-scipipe-12.0.0-exact) $ setup lsst_distrib
   (lsst-scipipe-12.0.0-exact) $ setup -r <path_to>/bps_parsl_sites -j

To configure `bps` to use parsl and the `SlurmWorkQueue` site configuration in this package, a code block like the following can be added to the bps yaml config file:

.. code-block:: yaml

   wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
   #computeSite: local
   computeSite: work_queue

   parsl:
     log_level: WARN

  site:
    local:
      class: lsst.ctrl.bps.parsl.sites.Local
      cores: 8
      monitorEnable: true
      monitorFilename: runinfo/monitoring.db
    work_queue:
      class: bps_parsl_sites.SlurmWorkQueue
      nodes_per_block: 3
      walltime: "00:30:00"
      exclusive: true
      qos: debug
      constraint: cpu
      scheduler_options: |
        #SBATCH --module=cvmfs
      worker_options: "--memory=480000"  # total memory in MB
      monitorEnable: true
      monitorFilename: runinfo/monitoring.db

Here are notes on some of the entries in this configuration block:

**computeSite**
  This can be used to switch between the `local` config,
  which is useful for running locally on a single, e.g., on a laptop, and
  the `work_queue` config, which will submit jobs to slurm, typically from
  a perlmutter login node.

**parsl.log_level**
  This controls the log-level of parsl output, which can
  be rather verbose, and this is separate from the logging control of the BPS
  software.  Unfortunately, there is also root-level parsl logging that
  can't be directly controlled without also affecting the bps log output.

**monitorFilename**
  This specifies the location of the sqlite3 monintoring.db file.  Enabling
  monitoring is useful for determining the state of the overall workflow.

**site.local.cores**
  These are the number of cores to use on the local node
  for running jobs.  Per-task memory requests ignored for running with
  the `local` `computeSite`.

**site.work_queue**
  * **nodes_per_block**:  The number of full batch nodes to allocate for
    running the workflow jobs.
  * **walltime**:  The walltime request for each batch submission. This must
    have the format "hours:minutes:seconds" since parsl tokenizes this string
    into three fields and will raise an error if it doesn't find all three.
  * **exclusive**:  Whether exclusive nodes are used.  This should probably be
    set to `true` for running on Perlmutter.
  * **qos**:  This sets the job's quality-of-service.  Set this to `debug`
    for running in the debug queue, to `regular` for the standard charge
    factor, etc..
  * **constraint**:  Set this to `cpu`.
  * **scheduler_options**:  These are additional entries, not covered by the
    above parameters, to add to the sbatch submission script generated
    by parsl.
  * **worker_options**:  These are options to pass to the `WorkQueueExecutor`
    to tell it what resources are available for running pipeline jobs.
    Perlmutter nodes have 512GB of RAM available, so passing `--memory=480000`
    here reserves 32GB for non-pipeline processes running on the node.
