"""
Functions for calling raw tools in the UCSC Computational Genomics Lab
ADAM/Spark pipeline

@author Audrey Musselman-Brown, almussel@ucsc.edu
@author Frank Austin Nothaft, fnothaft@berkeley.
"""

import os.path
from subprocess import check_call
import time

from toil.lib.docker import dockerCall

from toil_lib import require

SPARK_MASTER_PORT = "7077"
HDFS_MASTER_PORT = "8020"


class MasterAddress(str):
    """
    A string containing the hostname or IP of the Spark/HDFS master. The Spark master expects its own address to
    match what the client uses to connect to it. For example, if the master is configured with a host name,
    the driver can't use an IP address to connect to it, and vice versa. This class works around by distinguishing
    between the notional master address (self) and the actual one (self.actual) and adds support for the special
    master address "auto" in order to implement auto-discovery of the master of a standalone.

    >>> foo = MasterAddress('foo')
    >>> foo == 'foo'
    True
    >>> foo.actual == 'foo'
    True
    >>> foo.actual == foo
    True
    """
    def __init__(self, master_ip):
        super(MasterAddress, self).__init__(master_ip)
        self.actual = self

    def docker_parameters(self, docker_parameters=None):
        """
        Augment a list of "docker run" arguments with those needed to map the  notional Spark master address to the
        real one, if they are different.
        """
        if self != self.actual:
            add_host_option = '--add-host=spark-master:' + self.actual
            if docker_parameters is None:
                docker_parameters = [add_host_option]
            else:
                docker_parameters.append(add_host_option)
        return docker_parameters

def _make_parameters(master_ip, default_parameters, memory, arguments, override_parameters):
    """
    Makes a Spark Submit style job submission line.

    :param masterIP: The Spark leader IP address.
    :param default_parameters: Application specific Spark configuration parameters.
    :param memory: The memory to allocate to each Spark driver and executor.
    :param arguments: Arguments to pass to the submitted job.
    :param override_parameters: Parameters passed by the user, that override our defaults.
    
    :type masterIP: MasterAddress
    :type default_parameters: list of string
    :type arguments: list of string
    :type memory: int or None
    :type override_parameters: list of string or None
    """

    # python doesn't support logical xor?
    # anywho, exactly one of memory or override_parameters must be defined
    require((override_parameters is not None or memory is not None) and
            (override_parameters is None or memory is None),
            "Either the memory setting must be defined or you must provide Spark configuration parameters.")
    
    # if the user hasn't provided overrides, set our defaults
    parameters = []
    if memory is not None:
        parameters = ["--master", "spark://%s:%s" % (master_ip, SPARK_MASTER_PORT),
                      "--conf", "spark.driver.memory=%s" % memory,
                      "--conf", "spark.executor.memory=%s" % memory,
                      "--conf", ("spark.hadoop.fs.default.name=hdfs://%s:%s" % (master_ip, HDFS_MASTER_PORT))]
    else:
        parameters.extend(override_parameters)

    # add the tool specific spark parameters
    parameters.extend(default_parameters)

    # spark submit expects a '--' to split the spark conf arguments from tool arguments
    parameters.append('--')

    # now add the tool arguments and return
    parameters.extend(arguments)

    return parameters        


def _format_time(start_time, end_time):
    """
    Formats an elapsed runtime as %dh%dm%ds.

    :param float start_time: The starting time of the interval in seconds.
    :param float end_time: The ending time of the interval in seconds.
    :return str: String represnting the runtime in hours, minutes, and seconds.
    """

    elapsed_time = end_time - start_time
    elapsed_hours = int(elapsed_time / 3600.0)
    elapsed_minutes = int((elapsed_time - float(elapsed_hours * 3600.0)) / 60.0)
    elapsed_seconds = int((elapsed_time - float(elapsed_hours * 3600.0 + elapsed_minutes * 60.0)))

    return ("%dh %dm %ds" % (elapsed_hours, elapsed_minutes, elapsed_hours))


def _log_container_execution(job, container, start_time, end_time, parameters):
    """
    Logs the runtime and arguments passed to a container back to the toil leader.

    :param toil.job.Job job: The toil job being run, used to access the filestore.
    :param str container: The name of the container being run.
    :param float start_time: The starting time of the interval in seconds.
    :param float end_time: The ending time of the interval in seconds.
    :param list[str] parameters: The command line parameters passed to the container.
    """

    runtime_string = _format_time(start_time, end_time)
    job.fileStore.logToMaster("Container %s ran in %s with parameters %r." % (container,
                                                                              runtime_string,
                                                                              parameters))

def call_conductor(job,
                   master_ip,
                   src,
                   dst,
                   container="quay.io/ucsc_cgl/conductor",
                   memory=None, override_parameters=None):
    """
    Invokes the Conductor container to copy files between S3 and HDFS and vice versa.
    Find Conductor at https://github.com/BD2KGenomics/conductor.

    :param toil.Job.job job: The Toil Job calling this function
    :param masterIP: The Spark leader IP address.
    :param src: URL of file to copy.
    :param dst: URL of location to copy file to.
    :param container: The container name to run.
    :param memory: Gigabytes of memory to provision for Spark driver/worker.
    :param override_parameters: Parameters passed by the user, that override our defaults.

    :type masterIP: MasterAddress
    :type src: string
    :type dst: string
    :type container: string
    :type memory: int or None
    :type override_parameters: list of string or None
    """

    arguments = ["-C", src, dst]

    docker_parameters = ['--log-driver', 'none', '--net=host']
    parameters = _make_parameters(master_ip,
                                  [], # no conductor specific spark configuration
                                  memory,
                                  arguments,
                                  override_parameters)

    start_time = time.time()
    dockerCall(job=job,
               tool=container,
               parameters=parameters,
               dockerParameters=docker_parameters)
    end_time = time.time()

    _log_container_execution(job, container, start_time, end_time, parameters)


def call_adam(job, master_ip, arguments,
              container="quay.io/ucsc_cgl/adam:0.22.0--7add8b306862902b2bdd28a991e4e8dbc5292504",
              memory=None,
              override_parameters=None,
              run_local=False,
              native_adam_path=None):
    """
    Invokes the ADAM container. Find ADAM at https://github.com/bigdatagenomics/adam.

    :param toil.Job.job job: The Toil Job calling this function
    :param masterIP: The Spark leader IP address.
    :param arguments: Arguments to pass to ADAM.
    :param container: The container name to run.
    :param memory: Gigabytes of memory to provision for Spark driver/worker.
    :param override_parameters: Parameters passed by the user, that override our defaults.
    :param native_adam_path: Path to ADAM executable. If not provided, Docker is used.
    :param run_local: If true, runs Spark with the --master local[*] setting, which uses
      all cores on the local machine. The master_ip will be disregarded.

    :type masterIP: MasterAddress
    :type arguments: list of string
    :type container: string
    :type memory: int or None
    :type override_parameters: list of string or None
    :type native_adam_path: string or None
    :type run_local: boolean
    """
    if run_local:
        master = ["--master", "local[*]"]
    else:
        master = ["--master",
                  ("spark://%s:%s" % (master_ip, SPARK_MASTER_PORT)),
                  "--conf", ("spark.hadoop.fs.default.name=hdfs://%s:%s" % (master_ip, HDFS_MASTER_PORT)),]

    default_params = (master + [
            # set max result size to unlimited, see #177
            "--conf", "spark.driver.maxResultSize=0",
            # these memory tuning parameters were derived in the course of running the
            # experiments for the ADAM sigmod paper:
            #
            # Nothaft, Frank Austin, et al. "Rethinking data-intensive science using scalable
            # analytics systems." Proceedings of the 2015 ACM SIGMOD International Conference
            # on Management of Data. ACM, 2015.
            #
            # the memory tunings reduce the amount of memory dedicated to caching, which we don't
            # take advantage of, and the network timeout flag reduces the number of job failures
            # caused by heavy gc load
            "--conf", "spark.storage.memoryFraction=0.3",
            "--conf", "spark.storage.unrollFraction=0.1",
            "--conf", "spark.network.timeout=300s"])

    # are we running adam via docker, or do we have a native path?
    if native_adam_path is None:
        docker_parameters = ['--log-driver', 'none', '--net=host']
        parameters = _make_parameters(master_ip,
                                                default_params,
                                                memory,
                                                arguments,
                                                override_parameters)

        start_time = time.time()
        dockerCall(job=job,
                   tool=container,
                   dockerParameters=docker_parameters,
                   parameters=parameters)
        end_time = time.time()

        _log_container_execution(job, container, start_time, end_time, parameters)

    else:
        check_call([os.path.join(native_adam_path, "bin/adam-submit")] +
                   default_params +
                   arguments)


def call_avocado(job, master_ip, arguments,
                 container="quay.io/ucsc_cgl/avocado:fb20657172d2ce38e5dcd5542b0915db4de7eaa0--036b9354dbd46e62c4d326b4308c4786fc966d6a",
                 memory=None,
                 override_parameters=None,
                 run_local=False):
    """
    Invokes the Avocado container. Find Avocado at https://github.com/bigdatagenomics/avocado.

    :param toil.Job.job job: The Toil Job calling this function
    :param masterIP: The Spark leader IP address.
    :param arguments: Arguments to pass to Avocado.
    :param container: The container name to run.
    :param memory: Gigabytes of memory to provision for Spark driver/worker.
    :param override_parameters: Parameters passed by the user, that override our defaults.
    :param run_local: If true, runs Spark with the --master local[*] setting, which uses
      all cores on the local machine. The master_ip will be disregarded.

    :type masterIP: MasterAddress
    :type arguments: list of string
    :type container: string
    :type memory: int or None
    :type override_parameters: list of string or None
    :type run_local: boolean
    """
    if run_local:
        master = ["--master", "local[*]"]
    else:
        master = ["--master",
                  ("spark://%s:%s" % (master_ip, SPARK_MASTER_PORT)),
                  "--conf", ("spark.hadoop.fs.default.name=hdfs://%s:%s" % (master_ip, HDFS_MASTER_PORT)),]

    default_params = (master + [
            # set max result size to unlimited, see #177
            "--conf", "spark.driver.maxResultSize=0",
            "--conf", "spark.kryoserializer.buffer.max=2047m"
            ])

    docker_parameters = ['--log-driver', 'none', '--net=host']
    parameters = _make_parameters(master_ip,
                                  default_params,
                                  memory,
                                  arguments,
                                  override_parameters)

    start_time = time.time()
    dockerCall(job=job,
               tool=container,
               dockerParameters=docker_parameters,
               parameters=parameters)
    end_time = time.time()

    _log_container_execution(job, container, start_time, end_time, parameters)


def call_cannoli(job, master_ip, arguments,
                 container="quay.io/ucsc_cgl/cannoli:0a9321a382fdfad1411cb308a0de1566bf4c8bb4--036b9354dbd46e62c4d326b4308c4786fc966d6a",
                 memory=None,
                 override_parameters=None,
                 run_local=False):
    """
    Invokes the Cannoli container. Find Cannoli at https://github.com/bigdatagenomics/cannoli.

    :param toil.Job.job job: The Toil Job calling this function
    :param masterIP: The Spark leader IP address.
    :param arguments: Arguments to pass to Cannoli.
    :param container: The container name to run.
    :param memory: Gigabytes of memory to provision for Spark driver/worker.
    :param override_parameters: Parameters passed by the user, that override our defaults.
    :param run_local: If true, runs Spark with the --master local[*] setting, which uses
      all cores on the local machine. The master_ip will be disregarded.

    :type masterIP: MasterAddress
    :type arguments: list of string
    :type container: string
    :type memory: int or None
    :type override_parameters: list of string or None
    :type run_local: boolean
    """
    if run_local:
        master = ["--master", "local[*]"]
    else:
        master = ["--master",
                  ("spark://%s:%s" % (master_ip, SPARK_MASTER_PORT)),
                  "--jars", "/opt/cgl-docker-lib/cannoli/target/cannoli-spark2_2.11-0.1-SNAPSHOT.jar",
                  "--conf", ("spark.hadoop.fs.default.name=hdfs://%s:%s" % (master_ip, HDFS_MASTER_PORT)),]

    docker_parameters = ['--log-driver', 'none', '--net=host']
    parameters = _make_parameters(master_ip,
                                  master,
                                  memory,
                                  arguments,
                                  override_parameters)

    start_time = time.time()
    dockerCall(job=job,
               tool=container,
               dockerParameters=docker_parameters,
               parameters=parameters)
    end_time = time.time()

    _log_container_execution(job, container, start_time, end_time, parameters)
