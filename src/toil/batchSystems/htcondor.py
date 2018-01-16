from __future__ import absolute_import
from builtins import str

import sys
import os
import logging
import time
import math

from six.moves.queue import Queue
from threading import Thread

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
                jobID, cpu, memory, disk, jobName, command = self.waitingJobs.pop(0)

                # prepare the htcondor.Submit object
                submitObj = self.prepareSubmission(cpu, memory, disk,
                                                       jobID, jobName, command)
                logger.debug("Submitting %r", submitObj)

                # submit job and get batch system ID (i.e. clusterID)
                batchJobID = self.submitJob(submitObj)
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

        def prepareSubmission(self, cpu, memory, disk, jobID, jobName, command):

            # convert resource requests
            cpu = int(math.ceil(cpu)) # integer CPUs
            memory = float(memory)/1024 # memory in KB
            disk = float(disk)/1024 # disk in KB

            # workaround htcondor python bindings unicode bug
            command = command.encode('utf8')

            # execute the entire command as /bin/sh -c "command"
            # for now, only transfer the executable and only transfer it
            # if its path is explicitly referenced in the command
            input_files = []
            tokens = command.split()
            if os.path.isfile(tokens[0]):
                input_files.append(tokens[0])

            submit_parameters = {
                'executable': '/bin/sh',
                'transfer_executable': 'False',
                'arguments': '''"-c '{0}'"'''.format(command),
                'environment': self.getEnvString(),
                'transfer_input_files': ' '.join(input_files),
                'request_cpus': '{0}'.format(cpu),
                'request_memory': '{0:.3f}KB'.format(memory),
                'request_disk': '{0:.3f}KB'.format(disk),
                'leave_in_queue': '(JobStatus == 4)',
                '+IsToilJob': 'True',
                '+ToilJobID': '{0}'.format(jobID),
                '+ToilJobName': '"{0}"'.format(jobName),
                '+ToilJobKilled': 'False',
            }

            return htcondor.Submit(submit_parameters)

        def submitJob(self, submitObj):
            schedd = self.connectSchedd()
            with schedd.transaction() as txn:
                batchJobID = submitObj.queue(txn)
            return batchJobID

        def getRunningJobIDs(self):
            requirements = '(JobStatus == 2) && (IsToilJob)'
            projection = ['ClusterId', 'ToilJobID', 'EnteredCurrentStatus']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,
                                    projection = projection)

            batchJobIDs = [v[0] for v in self.batchJobIDs.values()]
            job_runtimes = {}
            for ad in ads:
                batchJobID = int(ad['ClusterId'])
                jobID = int(ad['ToilJobID'])
                if not (batchJobID in batchJobIDs):
                    continue
                runtime = time.time() - ad['EnteredCurrentStatus']
                job_runtimes[jobID] = runtime

            return job_runtimes

        def killJob(self, jobID):
            batchJobID = self.batchJobIDs[jobID][0]
            logger.debug("Killing HTCondor job {0}".format(batchJobID))

            # Set the job to be killed when its exit status is checked
            schedd = self.connectSchedd()
            job_spec = '(ClusterId == {0})'.format(batchJobID)
            schedd.edit(job_spec, 'ToilJobKilled', 'True')

        def getJobExitCode(self, batchJobID):
            logger.debug("Getting exit code for HTCondor job {0}".format(batchJobID))

            status = {
                1: 'Idle',
                2: 'Running',
                3: 'Removed',
                4: 'Completed',
                5: 'Held',
                6: 'Transferring Output',
                7: 'Suspended'
            }

            requirements = '(ClusterId == {0})'.format(batchJobID)
            projection = ['JobStatus', 'ToilJobKilled',
                              'ExitCode', 'HoldReason', 'HoldReasonSubCode']

            schedd = self.connectSchedd()
            ads = schedd.xquery(requirements = requirements,
                                    projection = projection)

            # Make sure a ClassAd was returned
            try:
                ad = ads.next()
            except StopIteration:
                logger.error(
                    "No HTCondor ads returned using constraint: {0}".format(
                        requirements))
                raise

            # Make sure only one ClassAd was returned
            try:
                ads.next()
            except StopIteration:
                pass
            else:
                logger.warning(
                    "Multiple HTCondor ads returned using constraint: {0}".format(
                        requirements))

            if ad['ToilJobKilled']:
                logger.debug("HTCondor job {0} was killed by Toil".format(
                    batchJobID))

                # Remove the job from the Schedd and return 1
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            if status[ad['JobStatus']] == 'Completed':
                logger.debug("HTCondor job {0} completed with exit code {1}".format(
                    batchJobID, ad['ExitCode']))

                # Remove the job from the Schedd and return its exit code
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return int(ad['ExitCode'])

            elif status[ad['JobStatus']] == 'Held':
                logger.error("HTCondor job {0} was held: '{1} (sub code {2})'".format(
                    batchJobID, ad['HoldReason'], ad['HoldReasonSubCode']))

                # Remove the job from the Schedd and return 1
                job_spec = 'ClusterId == {0}'.format(batchJobID)
                schedd.act(htcondor.JobAction.Remove, job_spec)
                return 1

            else: # Job still running or idle or doing something else
                logger.debug("HTCondor job {0} has not completed (Status: {1})".format(
                    batchJobID, status[ad['JobStatus']]))
                return None


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
                    "Connecting to HTCondor Schedd {0} using Collector at {1}".format(
                    schedd_name, condor_host))
                try:
                    schedd_ad = htcondor.Collector(condor_host).locate(
                        htcondor.DaemonTypes.Schedd, schedd_name)
                except IOError:
                    logger.error(
                        "Could not connect to HTCondor Collector at {0}".format(
                            condor_host))
                    raise
                except ValueError:
                    logger.error(
                        "Could not find HTCondor Schedd with name {0}".format(
                            schedd_name))
                    raise
                else:
                    schedd = htcondor.Schedd(schedd_ad)

            # otherwise assume the schedd is on the local machine
            else:
                logger.debug("Connecting to HTCondor Schedd on local machine")
                schedd = htcondor.Schedd()

            # ping the schedd
            try:
                schedd.xquery(limit = 0)
            except RuntimeError:
                logger.error("Could not connect to HTCondor Schedd")
                raise

            return schedd

        def getEnvString(self):
            '''Build environment string that HTCondor Submit object can use'''
            env_items = []
            if self.boss.environment:
                for key, value in self.boss.environment.items():
                    env_string = key + "="
                    # surround value with single quotes
                    # repeat any quote marks inside value
                    env_string += "'" + value.replace("'", "''").replace('"', '""') + "'"
                    env_items.append(env_string)
            # surround entire string with double quotes
            # separate key=value pairs with spaces
            return '"' + ' '.join(env_items) + '"'

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
        localID = self.handleLocalJob(jobNode)
        if localID:
            return localID
        else:
            # HTCondor does not need us to check resources
            #self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
            jobID = self.getNextJobID()
            self.currentJobs.add(jobID)
            self.newJobsQueue.put((jobID, jobNode.cores, jobNode.memory, jobNode.disk,
                                       jobNode.jobName, jobNode.command))
            logger.debug("Issued the job command: %s with job id: %s ",
                             jobNode.command, str(jobID))
        return jobID

    @classmethod
    def obtainSystemConstants(cls):
        logger.debug("HTCondor does not need to assess global cluster resources.")
        return None, None

    @classmethod
    def getWaitDuration(self):
        return 5
