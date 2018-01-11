from __future__ import absolute_import
from builtins import str

import sys
import os
import logging
import time
import math

from six.moves.queue import Queue
from threading import Thread

try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()

from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem
from toil.batchSystems import registry

import htcondor
import classad

logger = logging.getLogger(__name__)

class HTCondorBatchSystem(AbstractGridEngineBatchSystem):
    # When using HTCondor, the Schedd handles scheduling

    class Worker(AbstractGridEngineBatchSystem.Worker):

        # override createJobs method so we can use htcondor.Submit objects
        # and so we can get disk allocation requests and ceil the cpu request
        def createJobs(self, newJob):
            activity = False

            if newJob is not None:
                self.waitingJobs.append(newJob)

            # Queue jobs as necessary:
            while len(self.waitingJobs) > 0:
                activity = True
                jobID, cpu, memory, disk, command = self.waitingJobs.pop(0)

                # prepare the htcondor.Submit object
                subObj = self.prepareSubmission(cpu, memory, disk, jobID, command)
                logger.debug("Submitting %r", subObj)

                # submit job and get batch system ID (i.e. clusterID)
                batchJobID = self.submitJob(subObj)
                logger.debug("Submitted job %s", str(batchJobID))

                # Store dict for mapping Toil job ID to batch job ID
                # TODO: Note that this currently stores a tuple of (batch system
                # ID, Task), but the second value is None by default and doesn't
                # seem to be used
                self.batchJobIDs[jobID] = (batchJobID, None)

                # Add to queue of queued ("running") jobs
                self.runningJobs.add(jobID)

                # Add to allocated resources
                self.allocatedCpus[jobID] = int(math.ceil(cpu))

            return activity

        def prepareSubmission(self, cpu, memory, disk, jobID, command):
            cpu = int(math.ceil(cpu)) # integer CPUs only
            memory = float(memory)/1024 # memory in KB
            disk = float(disk)/1024 # disk in KB

            executable = command.split()[0].encode('utf8')
            arguments = command[len(executable):].lstrip().encode('utf8')

            sub = {
                'executable': executable,
                'arguments': arguments,
                'request_cpus': '{0}'.format(cpu),
                'request_memory': '{0}KB'.format(memory),
                'request_disk': '{0}KB'.format(disk),
                'leave_in_queue': '(JobStatus == 4)',
                'periodic_remove': '(JobStatus == 5)',
                '+IsToilJob': 'True',
                '+ToilJobID': '{0}'.format(jobID),
                }

            return htcondor.Submit(sub)

        def submitJob(self, subObj):
            schedd = self.connectSchedd()
            with schedd.transaction() as txn:
                batchJobID = subObj.queue(txn)
            return batchJobID

        def getRunningJobIDs(self):
            requirements = '(JobStatus == 2) && (IsToilJob)'
            projection = ['ToilJobID', 'EnteredCurrentStatus']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,
                                    projection = projection)

            job_runtimes = {}
            for ad in ads:
                jobID = ad['ToilJobID']
                if not (jobID in self.runningJobs):
                    continue
                runtime = time.time() - ad['EnteredCurrentStatus']
                job_runtimes[jobID] = runtime

            return job_runtimes

        def killJob(self, jobID):
            clusterID = self.getBatchSystemID(jobID)
            schedd = connectSchedd()
            schedd.act(htcondor.JobAction.Remove, str(clusterID))

        def getJobExitCode(self, batchJobID):
            logger.debug("Getting exit code for HTCondor job {0}".format(batchJobID))
            requirements = '(ClusterId == {0})'.format(batchJobID)
            projection = ['JobStatus', 'ExitCode']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,
                                    projection = projection)

            ad = ads.next()
            try:
                ads.next()
            except StopIteration:
                pass
            else:
                logger.warning(
                    "Warning: multiple ads returned using constraint: {0}".format(
                        requirements))

            # If the job is complete, then JobStatus == 4
            if ad['JobStatus'] != 4:
                logger.debug("HTCondor job {0} has not completed".format(batchJobID))
                return None
            else:
                # Remove the job from the Schedd
                schedd.act(htcondor.JobAction.Remove, str(batchJobID))
                return int(ad['ExitCode'])


        """
        Implementation-specific helper methods
        """

        def connectSchedd(self):
            '''Connect to HTCondor Schedd and return a Schedd object'''

            condor_host = os.getenv('TOIL_HTCONDOR_COLLECTOR')
            schedd_name = os.getenv('TOIL_HTCONDOR_SCHEDD')

            # if env variables set, use them to find the schedd
            if condor_host and schedd_name:
                logger.debug(
                    "connecting to HTCondor Schedd {0} using Collector at {1}".format(
                    schedd_name, condor_host))
                try:
                    schedd_ad = htcondor.Collector(condor_host).locate(
                        htcondor.DaemonTypes.Schedd, schedd_name)
                except IOError:
                    logger.error(
                        "could not connect to HTCondor Collector at {0}".format(
                            condor_host))
                    raise
                except ValueError:
                    logger.error(
                        "could not find HTCondor Schedd with name {0}".format(
                            schedd_name))
                    raise
                else:
                    schedd = htcondor.Schedd(schedd_ad)

            # otherwise assume the schedd is on the local machine
            else:
                logger.debug("connecting to HTCondor Schedd on local machine")
                schedd = htcondor.Schedd()

            # ping the schedd
            try:
                schedd.xquery(limit = 0)
            except RuntimeError:
                logger.error("could not connect to HTCondor Schedd")
                raise

            return schedd

    # override method to remove resource request constraints
    # HTCondor will handle resource requests
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        self.config = config
        self.environment = {}

        self.currentJobs = set()
        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        # get the associated worker class here
        self.worker = self.Worker(self.newJobsQueue, self.updatedJobsQueue,
                                      self.killQueue, self.killedJobsQueue, self)
        self.worker.start()
        self.localBatch = registry.batchSystemFactoryFor(
            registry.defaultBatchSystem())()(config, maxCores, maxMemory, maxDisk)
        self._getRunningBatchJobIDsTimestamp = None
        self._getRunningBatchJobIDsCache = {}

    # override issueBatchJob method so we can get disk allocation requests
    # and remove resource request constraints
    def issueBatchJob(self, jobNode):
        # Avoid submitting internal jobs to the batch queue, handle locally
        if jobNode.jobName.startswith(CWL_INTERNAL_JOBS):
            jobID = self.localBatch.issueBatchJob(jobNode)
        else:
            # HTCondor does not need us to check resources
            #self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
            with self.localBatch.jobIndexLock:
                jobID = self.localBatch.jobIndex
                self.localBatch.jobIndex += 1
            self.currentJobs.add(jobID)
            self.newJobsQueue.put((jobID, jobNode.cores, jobNode.memory,
                                       jobNode.disk, jobNode.command))
            logger.debug("Issued the job command: %s with job id: %s ",
                             jobNode.command, str(jobID))
        return jobID

    @classmethod
    def obtainSystemConstants(cls):
        logger.debug("HTCondor does not need obtainSystemConstants to assess global cluster resources.")
        return None, None

    @classmethod
    def getWaitDuration(self):
        return 5
