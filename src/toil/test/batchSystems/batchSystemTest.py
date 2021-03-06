# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from builtins import map
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
from abc import ABCMeta, abstractmethod
from fractions import Fraction
from inspect import getsource
import logging
import os
import fcntl
import itertools
import tempfile
from textwrap import dedent
import time
import multiprocessing
import sys
import subprocess
from unittest import skipIf

from toil.common import Config
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.batchSystems.parasolTestSupport import ParasolTestSupport
from toil.batchSystems.parasol import ParasolBatchSystem
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.batchSystems.abstractBatchSystem import (InsufficientSystemResources,
                                                   BatchSystemSupport)
from toil.job import Job, JobNode
from toil.test import (ToilTest,
                       needs_mesos,
                       needs_parasol,
                       needs_gridengine,
                       needs_slurm,
                       needs_torque,
                       needs_htcondor,
                       slow,
                       tempFileContaining)
from future.utils import with_metaclass

log = logging.getLogger(__name__)

# How many cores should be utilized by this test. The test will fail if the running system
# doesn't have at least that many cores.

numCores = 2

preemptable = False

defaultRequirements = dict(memory=int(100e6), cores=1, disk=1000, preemptable=preemptable)


class hidden(object):
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractBatchSystemTest(with_metaclass(ABCMeta, ToilTest)):
        """
        A base test case with generic tests that every batch system should pass
        """

        @abstractmethod
        def createBatchSystem(self):
            """
            :rtype: AbstractBatchSystem
            """
            raise NotImplementedError

        def supportsWallTime(self):
            return False

        @classmethod
        def createConfig(cls):
            """
            Returns a dummy config for the batch system tests.  We need a workflowID to be set up
            since we are running tests without setting up a jobstore. This is the class version
            to be used when an instance is not available.

            :rtype: toil.common.Config
            """
            config = Config()
            from uuid import uuid4
            config.workflowID = str(uuid4())
            return config

        def _createConfig(self):
            """
            Returns a dummy config for the batch system tests.  We need a workflowID to be set up
            since we are running tests without setting up a jobstore.

            :rtype: toil.common.Config
            """
            return self.createConfig()

        @classmethod
        def setUpClass(cls):
            super(hidden.AbstractBatchSystemTest, cls).setUpClass()
            logging.basicConfig(level=logging.DEBUG)

        def setUp(self):
            super(hidden.AbstractBatchSystemTest, self).setUp()
            self.config = self._createConfig()
            self.batchSystem = self.createBatchSystem()
            self.tempDir = self._createTempDir('testFiles')

        def tearDown(self):
            self.batchSystem.shutdown()
            super(hidden.AbstractBatchSystemTest, self).tearDown()

        def testAvailableCores(self):
            self.assertTrue(multiprocessing.cpu_count() >= numCores)

        def testRunJobs(self):
            testPath = os.path.join(self.tempDir, "test.txt")
            jobNode1 = JobNode(command='sleep 1000', jobName='test1', unitName=None,
                               jobStoreID='1', requirements=defaultRequirements)
            jobNode2 = JobNode(command='sleep 1000', jobName='test2', unitName=None,
                               jobStoreID='2', requirements=defaultRequirements)
            job1 = self.batchSystem.issueBatchJob(jobNode1)
            job2 = self.batchSystem.issueBatchJob(jobNode2)

            issuedIDs = self._waitForJobsToIssue(2)
            self.assertEqual(set(issuedIDs), {job1, job2})

            runningJobIDs = self._waitForJobsToStart(2)
            self.assertEqual(set(runningJobIDs), {job1, job2})

            # Killing the jobs instead of allowing them to complete means this test can run very
            # quickly if the batch system issues and starts the jobs quickly.
            self.batchSystem.killBatchJobs([job1, job2])
            self.assertEqual({}, self.batchSystem.getRunningBatchJobIDs())

            # Issue a job and then allow it to finish by itself, causing it to be added to the
            # updated jobs queue.
            self.assertFalse(os.path.exists(testPath))
            jobNode3 = JobNode(command="touch %s" % testPath, jobName='test3', unitName=None,
                               jobStoreID='3', requirements=defaultRequirements)
            job3 = self.batchSystem.issueBatchJob(jobNode3)

            jobID, exitStatus, wallTime = self.batchSystem.getUpdatedBatchJob(maxWait=1000)

            # Since the first two jobs were killed, the only job in the updated jobs queue should
            # be job 3. If the first two jobs were (incorrectly) added to the queue, this will
            # fail with jobID being equal to job1 or job2.
            self.assertEqual(exitStatus, 0)
            self.assertEqual(jobID, job3)
            if self.supportsWallTime():
                self.assertTrue(wallTime > 0)
            else:
                self.assertIsNone(wallTime)
            self.assertTrue(os.path.exists(testPath))
            self.assertFalse(self.batchSystem.getUpdatedBatchJob(0))

            # Make sure killBatchJobs can handle jobs that don't exist
            self.batchSystem.killBatchJobs([10])

        def testSetEnv(self):
            # Parasol disobeys shell rules and stupidly splits the command at the space character
            # before exec'ing it, whether the space is quoted, escaped or not. This means that we
            # can't have escaped or quotes spaces in the command line. So we can't use bash -c
            #  '...' or python -c '...'. The safest thing to do here is to script the test and
            # invoke that script rather than inline the test via -c.
            def assertEnv():
                import os, sys
                sys.exit(0 if os.getenv('FOO') == 'bar' else 42)

            script_body = dedent('\n'.join(getsource(assertEnv).split('\n')[1:]))
            with tempFileContaining(script_body, suffix='.py') as script_path:
                # First, ensure that the test fails if the variable is *not* set
                command = sys.executable + ' ' + script_path
                jobNode4 = JobNode(command=command, jobName='test4', unitName=None,
                                   jobStoreID='4', requirements=defaultRequirements)
                job4 = self.batchSystem.issueBatchJob(jobNode4)
                jobID, exitStatus, wallTime = self.batchSystem.getUpdatedBatchJob(maxWait=1000)
                self.assertEqual(exitStatus, 42)
                self.assertEqual(jobID, job4)
                # Now set the variable and ensure that it is present
                self.batchSystem.setEnv('FOO', 'bar')
                jobNode5 = JobNode(command=command, jobName='test5', unitName=None,
                                   jobStoreID='5', requirements=defaultRequirements)
                job5 = self.batchSystem.issueBatchJob(jobNode5)
                jobID, exitStatus, wallTime = self.batchSystem.getUpdatedBatchJob(maxWait=1000)
                self.assertEqual(exitStatus, 0)
                self.assertEqual(jobID, job5)

        def testCheckResourceRequest(self):
            if isinstance(self.batchSystem, BatchSystemSupport):
                checkResourceRequest = self.batchSystem.checkResourceRequest
                self.assertRaises(InsufficientSystemResources, checkResourceRequest,
                                  memory=1000, cores=200, disk=1e9)
                self.assertRaises(InsufficientSystemResources, checkResourceRequest,
                                  memory=5, cores=200, disk=1e9)
                self.assertRaises(InsufficientSystemResources, checkResourceRequest,
                                  memory=1001e9, cores=1, disk=1e9)
                self.assertRaises(InsufficientSystemResources, checkResourceRequest,
                                  memory=5, cores=1, disk=2e9)
                self.assertRaises(AssertionError, checkResourceRequest,
                                  memory=None, cores=1, disk=1000)
                self.assertRaises(AssertionError, checkResourceRequest,
                                  memory=10, cores=None, disk=1000)
                checkResourceRequest(memory=10, cores=1, disk=100)

        def testScalableBatchSystem(self):
            # If instance of scalable batch system
            pass

        def _waitForJobsToIssue(self, numJobs):
            issuedIDs = []
            for it in range(20):
                issuedIDs = self.batchSystem.getIssuedBatchJobIDs()
                if len(issuedIDs) == numJobs:
                    break
                time.sleep(1)
            return issuedIDs

        def _waitForJobsToStart(self, numJobs):
            runningIDs = []
            # prevent an endless loop, give it 20 tries
            for it in range(20):
                runningIDs = list(self.batchSystem.getRunningBatchJobIDs().keys())
                if len(runningIDs) == numJobs:
                    break
                time.sleep(1)
            return runningIDs

    class AbstractBatchSystemJobTest(with_metaclass(ABCMeta, ToilTest)):
        """
        An abstract base class for batch system tests that use a full Toil workflow rather
        than using the batch system directly.
        """

        cpuCount = multiprocessing.cpu_count()
        allocatedCores = sorted({1, 2, cpuCount})
        sleepTime = 5

        @abstractmethod
        def getBatchSystemName(self):
            """
            :rtype: (str, AbstractBatchSystem)
            """
            raise NotImplementedError

        def getOptions(self, tempDir):
            """
            Configures options for Toil workflow and makes job store.
            :param str tempDir: path to test directory
            :return: Toil options object
            """
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "DEBUG"
            options.batchSystem = self.batchSystemName
            options.workDir = tempDir
            options.maxCores = self.cpuCount
            return options

        def setUp(self):
            self.batchSystemName = self.getBatchSystemName()
            super(hidden.AbstractBatchSystemJobTest, self).setUp()

        def tearDown(self):
            super(hidden.AbstractBatchSystemJobTest, self).tearDown()

        @slow
        def testJobConcurrency(self):
            """
            Tests that the batch system is allocating core resources properly for concurrent tasks.
            """
            for coresPerJob in self.allocatedCores:
                tempDir = self._createTempDir('testFiles')
                options = self.getOptions(tempDir)

                counterPath = os.path.join(tempDir, 'counter')
                resetCounters(counterPath)
                value, maxValue = getCounters(counterPath)
                assert (value, maxValue) == (0, 0)

                root = Job()
                for _ in range(self.cpuCount):
                    root.addFollowOn(Job.wrapFn(measureConcurrency, counterPath, self.sleepTime,
                                                cores=coresPerJob, memory='1M', disk='1Mi'))
                Job.Runner.startToil(root, options)
                _, maxValue = getCounters(counterPath)
                self.assertEqual(maxValue, old_div(self.cpuCount, coresPerJob))

    class AbstractGridEngineBatchSystemTest(AbstractBatchSystemTest):
        """
        An abstract class to reduce redundancy between Grid Engine, Slurm, and other similar batch
        systems
        """

        def _createConfig(self):
            config = super(hidden.AbstractGridEngineBatchSystemTest, self)._createConfig()
            # can't use _getTestJobStorePath since that method removes the directory
            config.jobStore = 'file:' + self._createTempDir('jobStore')
            return config

        def testResultFile(self):
            """
            Tests that the result file name is formatted properly
            """
            # noinspection PyUnresolvedReferences
            fileName = self.batchSystem._getResultsFileName(self.config.jobStore)
            filePath, _ = os.path.split(fileName)  # removes file so dir matches config.jobStore
            locator = self.config.jobStore
            self.assertTrue(locator.startswith('file:'))
            self.assertEqual(locator[len('file:'):], filePath)


@slow
@needs_mesos
class MesosBatchSystemTest(hidden.AbstractBatchSystemTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    def createConfig(cls):
        """
        needs to set mesosMasterAddress to localhost for testing since the default is now the
        private IP address
        """
        config = super(MesosBatchSystemTest, cls).createConfig()
        config.mesosMasterAddress = 'localhost:5050'
        return config

    def supportsWallTime(self):
        return True

    def createBatchSystem(self):
        from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
        self._startMesos(numCores)
        return MesosBatchSystem(config=self.config,
                                maxCores=numCores, maxMemory=1e9, maxDisk=1001)

    def tearDown(self):
        self._stopMesos()
        super(MesosBatchSystemTest, self).tearDown()

    def testIgnoreNode(self):
        self.batchSystem.ignoreNode('localhost')
        jobNode = JobNode(command='sleep 1000', jobName='test2', unitName=None,
                           jobStoreID='1', requirements=defaultRequirements)
        job = self.batchSystem.issueBatchJob(jobNode)

        issuedID = self._waitForJobsToIssue(1)
        self.assertEqual(set(issuedID), {job})

        runningJobIDs = self._waitForJobsToStart(1)
        # Make sure job is NOT running
        self.assertEqual(set(runningJobIDs), set({}))


class SingleMachineBatchSystemTest(hidden.AbstractBatchSystemTest):
    """
    Tests against the single-machine batch system
    """

    def supportsWallTime(self):
        return True

    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config,
                                        maxCores=numCores, maxMemory=1e9, maxDisk=2001)


@slow
class MaxCoresSingleMachineBatchSystemTest(ToilTest):
    """
    This test ensures that single machine batch system doesn't exceed the configured number
    cores
    """

    @classmethod
    def setUpClass(cls):
        super(MaxCoresSingleMachineBatchSystemTest, cls).setUpClass()
        logging.basicConfig(level=logging.DEBUG)

    def setUp(self):
        super(MaxCoresSingleMachineBatchSystemTest, self).setUp()

        def writeTempFile(s):
            fd, path = tempfile.mkstemp()
            try:
                assert os.write(fd, s) == len(s)
            except:
                os.unlink(path)
                raise
            else:
                return path
            finally:
                os.close(fd)

        # Write initial value of counter file containing a tuple of two integers (i, n) where i
        # is the number of currently executing tasks and n the maximum observed value of i
        self.counterPath = writeTempFile('0,0')

        def script():
            import os, sys, fcntl, time
            def count(delta):
                """
                Adjust the first integer value in a file by the given amount. If the result
                exceeds the second integer value, set the second one to the first.
                """
                fd = os.open(sys.argv[1], os.O_RDWR)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                    try:
                        s = os.read(fd, 10)
                        value, maxValue = list(map(int, s.split(',')))
                        value += delta
                        if value > maxValue: maxValue = value
                        os.lseek(fd, 0, 0)
                        os.ftruncate(fd, 0)
                        os.write(fd, ','.join(map(str, (value, maxValue))))
                    finally:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)

            # Without the second argument, increment counter, sleep one second and decrement.
            # Othwerise, adjust the counter by the given delta, which can be useful for services.
            if len(sys.argv) < 3:
                count(1)
                try:
                    time.sleep(1)
                finally:
                    count(-1)
            else:
                count(int(sys.argv[2]))

        self.scriptPath = writeTempFile(dedent('\n'.join(getsource(script).split('\n')[1:])))

    def tearDown(self):
        os.unlink(self.scriptPath)
        os.unlink(self.counterPath)

    def scriptCommand(self):
        return ' '.join([sys.executable, self.scriptPath, self.counterPath])

    def test(self):
        # We'll use fractions to avoid rounding errors. Remember that not every fraction can be
        # represented as a floating point number.
        F = Fraction
        # This test isn't general enough to cover every possible value of minCores in
        # SingleMachineBatchSystem. Instead we hard-code a value and assert it.
        minCores = F(1, 10)
        self.assertEquals(float(minCores), SingleMachineBatchSystem.minCores)
        for maxCores in {F(minCores), minCores * 10, F(1), F(numCores, 2), F(numCores)}:
            for coresPerJob in {F(minCores), F(minCores * 10), F(1), F(maxCores, 2), F(maxCores)}:
                for load in (F(1, 10), F(1), F(10)):
                    jobs = int(maxCores / coresPerJob * load)
                    if jobs >= 1 and minCores <= coresPerJob < maxCores:
                        self.assertEquals(maxCores, float(maxCores))
                        bs = SingleMachineBatchSystem(
                            config=hidden.AbstractBatchSystemTest.createConfig(),
                            maxCores=float(maxCores),
                            # Ensure that memory or disk requirements don't get in the way.
                            maxMemory=jobs * 10,
                            maxDisk=jobs * 10)
                        try:
                            jobIds = set()
                            for i in range(0, int(jobs)):
                                jobIds.add(bs.issueBatchJob(JobNode(command=self.scriptCommand(),
                                                                    requirements=dict(
                                                                        cores=float( coresPerJob),
                                                                        memory=1, disk=1,
                                                                        preemptable=preemptable),
                                                                    jobName=str(i), unitName='', jobStoreID=str(i))))
                            self.assertEquals(len(jobIds), jobs)
                            while jobIds:
                                job = bs.getUpdatedBatchJob(maxWait=10)
                                self.assertIsNotNone(job)
                                jobId, status, wallTime = job
                                self.assertEquals(status, 0)
                                # would raise KeyError on absence
                                jobIds.remove(jobId)
                        finally:
                            bs.shutdown()
                        concurrentTasks, maxConcurrentTasks = getCounters(self.counterPath)
                        self.assertEquals(concurrentTasks, 0)
                        log.info('maxCores: {maxCores}, '
                                 'coresPerJob: {coresPerJob}, '
                                 'load: {load}'.format(**locals()))
                        # This is the key assertion:
                        expectedMaxConcurrentTasks = min(old_div(maxCores, coresPerJob), jobs)
                        self.assertEquals(maxConcurrentTasks, expectedMaxConcurrentTasks)
                        resetCounters(self.counterPath)

    @skipIf(SingleMachineBatchSystem.numCores < 3, 'Need at least three cores to run this test')
    def testServices(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logDebug = True
        options.maxCores = 3
        self.assertTrue(options.maxCores <= SingleMachineBatchSystem.numCores)
        Job.Runner.startToil(Job.wrapJobFn(parentJob, self.scriptCommand()), options)
        with open(self.counterPath, 'r+') as f:
            s = f.read()
        log.info('Counter is %s', s)
        self.assertEqual(getCounters(self.counterPath), (0, 3))


# Toil can use only top-level functions so we have to add them here:

def parentJob(job, cmd):
    job.addChildJobFn(childJob, cmd)


def childJob(job, cmd):
    job.addService(Service(cmd))
    job.addChildJobFn(grandChildJob, cmd)
    subprocess.check_call(cmd, shell=True)


def grandChildJob(job, cmd):
    job.addService(Service(cmd))
    job.addChildFn(greatGrandChild, cmd)
    subprocess.check_call(cmd, shell=True)


def greatGrandChild(cmd):
    subprocess.check_call(cmd, shell=True)


class Service(Job.Service):
    def __init__(self, cmd):
        super(Service, self).__init__()
        self.cmd = cmd

    def start(self, fileStore):
        subprocess.check_call(self.cmd + ' 1', shell=True)

    def check(self):
        return True

    def stop(self, fileStore):
        subprocess.check_call(self.cmd + ' -1', shell=True)


@slow
@needs_parasol
class ParasolBatchSystemTest(hidden.AbstractBatchSystemTest, ParasolTestSupport):
    """
    Tests the Parasol batch system
    """

    def supportsWallTime(self):
        return True

    def _createConfig(self):
        config = super(ParasolBatchSystemTest, self)._createConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        memory = int(3e9)
        self._startParasol(numCores=numCores, memory=memory)

        return ParasolBatchSystem(config=self.config,
                                  maxCores=numCores,
                                  maxMemory=memory,
                                  maxDisk=1001)

    def tearDown(self):
        super(ParasolBatchSystemTest, self).tearDown()
        self._stopParasol()

    def testBatchResourceLimits(self):
        jobNode1 = JobNode(command="sleep 1000",
                           requirements=dict(memory=1 << 30, cores=1,
                                             disk=1000, preemptable=preemptable),
                           jobName='testResourceLimits', unitName=None,
                           jobStoreID='1')
        job1 = self.batchSystem.issueBatchJob(jobNode1)
        self.assertIsNotNone(job1)
        jobNode2 = JobNode(command="sleep 1000",
                           requirements=dict(memory=2 << 30, cores=1,
                                             disk=1000, preemptable=preemptable),
                           jobName='testResourceLimits', unitName=None,
                           jobStoreID='2')
        job2 = self.batchSystem.issueBatchJob(jobNode2)
        self.assertIsNotNone(job2)
        batches = self._getBatchList()
        self.assertEqual(len(batches), 2)
        # It would be better to directly check that the batches have the correct memory and cpu
        # values, but Parasol seems to slightly change the values sometimes.
        self.assertNotEqual(batches[0]['ram'], batches[1]['ram'])
        # Need to kill one of the jobs because there are only two cores available
        self.batchSystem.killBatchJobs([job2])
        job3 = self.batchSystem.issueBatchJob(jobNode1)
        self.assertIsNotNone(job3)
        batches = self._getBatchList()
        self.assertEqual(len(batches), 1)

    def _parseBatchString(self, batchString):
        import re
        batchInfo = dict()
        memPattern = re.compile("(\d+\.\d+)([kgmbt])")
        items = batchString.split()
        batchInfo["cores"] = int(items[7])
        memMatch = memPattern.match(items[8])
        ramValue = float(memMatch.group(1))
        ramUnits = memMatch.group(2)
        ramConversion = {'b': 1e0, 'k': 1e3, 'm': 1e6, 'g': 1e9, 't': 1e12}
        batchInfo["ram"] = ramValue * ramConversion[ramUnits]
        return batchInfo

    def _getBatchList(self):
        # noinspection PyUnresolvedReferences
        exitStatus, batchLines = self.batchSystem._runParasol(['list', 'batches'])
        self.assertEqual(exitStatus, 0)
        return [self._parseBatchString(line) for line in batchLines[1:] if line]


@slow
@needs_gridengine
class GridEngineBatchSystemTest(hidden.AbstractGridEngineBatchSystemTest):
    """
    Tests against the GridEngine batch system
    """

    def createBatchSystem(self):
        from toil.batchSystems.gridengine import GridEngineBatchSystem
        return GridEngineBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9,
                                     maxDisk=1e9)

    def tearDown(self):
        super(GridEngineBatchSystemTest, self).tearDown()
        # Cleanup GridEngine output log file from qsub
        from glob import glob
        for f in glob('toil_job*.o*'):
            os.unlink(f)


@slow
@needs_slurm
class SlurmBatchSystemTest(hidden.AbstractGridEngineBatchSystemTest):
    """
    Tests against the Slurm batch system
    """

    def createBatchSystem(self):
        from toil.batchSystems.slurm import SlurmBatchSystem
        return SlurmBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9,
                                maxDisk=1e9)

    def tearDown(self):
        super(SlurmBatchSystemTest, self).tearDown()
        # Cleanup 'slurm-%j.out' produced by sbatch
        from glob import glob
        for f in glob('slurm-*.out'):
            os.unlink(f)


@slow
@needs_torque
class TorqueBatchSystemTest(hidden.AbstractGridEngineBatchSystemTest):
    """
    Tests against the Torque batch system
    """

    def _createDummyConfig(self):
        config = super(TorqueBatchSystemTest, self)._createDummyConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        from toil.batchSystems.torque import TorqueBatchSystem
        return TorqueBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9,
                                     maxDisk=1e9)

    def tearDown(self):
        super(TorqueBatchSystemTest, self).tearDown()
        # Cleanup 'toil_job-%j.out' produced by sbatch
        from glob import glob
        for f in glob('toil_job_*.[oe]*'):
            os.unlink(f)

@slow
@needs_htcondor
class HTCondorBatchSystemTest(hidden.AbstractGridEngineBatchSystemTest):
    """
    Tests against the HTCondor batch system
    """

    def createBatchSystem(self):
        from toil.batchSystems.htcondor import HTCondorBatchSystem
        return HTCondorBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9,
                                       maxDisk=1e9)

    def tearDown(self):
        super(HTCondorBatchSystemTest, self).tearDown()

class SingleMachineBatchSystemJobTest(hidden.AbstractBatchSystemJobTest):
    """
    Tests Toil workflow against the SingleMachine batch system
    """

    def getBatchSystemName(self):
        return "singleMachine"

    @slow
    def testConcurrencyWithDisk(self):
        """
        Tests that the batch system is allocating disk resources properly
        """
        tempDir = self._createTempDir('testFiles')

        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = tempDir
        from toil import physicalDisk
        availableDisk = physicalDisk('', toilWorkflowDir=options.workDir)
        options.batchSystem = self.batchSystemName

        counterPath = os.path.join(tempDir, 'counter')
        resetCounters(counterPath)
        value, maxValue = getCounters(counterPath)
        assert (value, maxValue) == (0, 0)

        root = Job()
        # Physically, we're asking for 50% of disk and 50% of disk + 500bytes in the two jobs. The
        # batchsystem should not allow the 2 child jobs to run concurrently.
        root.addChild(Job.wrapFn(measureConcurrency, counterPath, self.sleepTime, cores=1,
                                    memory='1M', disk=old_div(availableDisk,2)))
        root.addChild(Job.wrapFn(measureConcurrency, counterPath, self.sleepTime, cores=1,
                                 memory='1M', disk=(old_div(availableDisk, 2)) + 500))
        Job.Runner.startToil(root, options)
        _, maxValue = getCounters(counterPath)
        self.assertEqual(maxValue, 1)

    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    @slow
    def testNestedResourcesDoNotBlock(self):
        """
        Resources are requested in the order Memory > Cpu > Disk.
        Test that inavailability of cpus for one job that is scheduled does not block another job
        that can run.
        """
        tempDir = self._createTempDir('testFiles')

        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = tempDir
        options.maxCores = 4
        from toil import physicalMemory
        availableMemory = physicalMemory()
        options.batchSystem = self.batchSystemName

        outFile = os.path.join(tempDir, 'counter')
        open(outFile, 'w').close()

        root = Job()

        blocker = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=30, writeVal='b',
                             cores=2, memory='1M', disk='1M')
        firstJob = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=5, writeVal='fJ',
                              cores=1, memory='1M', disk='1M')
        secondJob = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=10,
                               writeVal='sJ', cores=1, memory='1M', disk='1M')

        # Should block off 50% of memory while waiting for it's 3 cores
        firstJobChild = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=0,
                                   writeVal='fJC', cores=3, memory=int(old_div(availableMemory,2)), disk='1M')

        # These two shouldn't be able to run before B because there should be only
        # (50% of memory - 1M) available (firstJobChild should be blocking 50%)
        secondJobChild = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=5,
                                    writeVal='sJC', cores=2, memory=int(old_div(availableMemory,1.5)),
                                    disk='1M')
        secondJobGrandChild = Job.wrapFn(_resourceBlockTestAuxFn, outFile=outFile, sleepTime=5,
                                         writeVal='sJGC', cores=2, memory=int(old_div(availableMemory,1.5)),
                                         disk='1M')

        root.addChild(blocker)
        root.addChild(firstJob)
        root.addChild(secondJob)

        firstJob.addChild(firstJobChild)
        secondJob.addChild(secondJobChild)

        secondJobChild.addChild(secondJobGrandChild)
        """
        The tree is:
                    root
                  /   |   \
                 b    fJ   sJ
                      |    |
                      fJC  sJC
                           |
                           sJGC
        But the order of execution should be
        root > b , fJ, sJ > sJC > sJGC > fJC
        since fJC cannot run till bl finishes but sJC and sJGC can(fJC blocked by disk). If the
        resource acquisition is written properly, then fJC which is scheduled before sJC and sJGC
        should not block them, and should only run after they finish.
        """
        Job.Runner.startToil(root, options)
        with open(outFile) as oFH:
            outString = oFH.read()
        # The ordering of b, fJ and sJ is non-deterministic since they are scheduled at the same
        # time. We look for all possible permutations.
        possibleStarts = tuple([''.join(x) for x in itertools.permutations(['b', 'fJ', 'sJ'])])
        assert outString.startswith(possibleStarts)
        assert outString.endswith('sJCsJGCfJC')


def _resourceBlockTestAuxFn(outFile, sleepTime, writeVal):
    """
    Write a value to the out file and then sleep for requested seconds.
    :param str outFile: File to write to
    :param int sleepTime: Time to sleep for
    :param str writeVal: Character to write
    """
    with open(outFile, 'a') as oFH:
        fcntl.flock(oFH, fcntl.LOCK_EX)
        oFH.write(writeVal)
    time.sleep(sleepTime)


@slow
@needs_mesos
class MesosBatchSystemJobTest(hidden.AbstractBatchSystemJobTest, MesosTestSupport):
    """
    Tests Toil workflow against the Mesos batch system
    """

    def getOptions(self, tempDir):
        options = super(MesosBatchSystemJobTest, self).getOptions(tempDir)
        options.mesosMasterAddress = 'localhost:5050'
        return options

    def getBatchSystemName(self):
        self._startMesos(self.cpuCount)
        return "mesos"

    def tearDown(self):
        self._stopMesos()


def measureConcurrency(filepath, sleep_time=5):
    """
    Run in parallel to determine the number of concurrent tasks.
    This code was copied from toil.batchSystemTestMaxCoresSingleMachineBatchSystemTest
    :param str filepath: path to counter file
    :param int sleep_time: number of seconds to sleep before counting down
    :return int max concurrency value:
    """
    count(1, filepath)
    try:
        time.sleep(sleep_time)
    finally:
        return count(-1, filepath)


def count(delta, file_path):
    """
    Increments counter file and returns the max number of times the file
    has been modified. Counter data must be in the form:
    concurrent tasks, max concurrent tasks (counter should be initialized to 0,0)

    :param int delta: increment value
    :param str file_path: path to shared counter file
    :return int max concurrent tasks:
    """
    fd = os.open(file_path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            s = os.read(fd, 10)
            value, maxValue = list(map(int, s.split(',')))
            value += delta
            if value > maxValue: maxValue = value
            os.lseek(fd, 0, 0)
            os.ftruncate(fd, 0)
            os.write(fd, ','.join(map(str, (value, maxValue))))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    return maxValue


def getCounters(path):
    with open(path, 'r+') as f:
        s = f.read()
        concurrentTasks, maxConcurrentTasks = list(map(int, s.split(',')))
    return concurrentTasks, maxConcurrentTasks


def resetCounters(path):
    with open(path, "w") as f:
        f.write("0,0")
        f.close()
