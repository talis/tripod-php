<?php

use PHPUnit\Framework\MockObject\MockObject;
use Tripod\Mongo\Jobs\EnsureIndexes;

class EnsureIndexesTest extends ResqueJobTestBase
{
    /**
     * @var array
     */
    protected $args = [];

    protected function setUp(): void
    {
        $this->args = [
            'tripodConfig' => '',
            'storeName' => '',
            'reindex' => '',
            'background' => '',
        ];
        parent::setUp();
    }

    /**
     * Test exception is thrown if mandatory arguments are not set
     *
     * @dataProvider mandatoryArgDataProvider
     * @group ensure-indexes
     * @throws Exception
     */
    public function testMandatoryArgs($argument, $argumentName = null)
    {
        if (!$argumentName) {
            $argumentName = $argument;
        }
        $job = new EnsureIndexes();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        unset($job->args[$argument]);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Argument {$argumentName} was not present in supplied job args for job Tripod\\Mongo\\Jobs\\EnsureIndexes");
        $this->performJob($job);
    }

    /**
     * Data provider for testMandatoryArgs
     *
     * @return array
     */
    public function mandatoryArgDataProvider()
    {
        return [
            ['tripodConfig', 'tripodConfig or tripodConfigGenerator'],
            ['storeName'],
            ['reindex'],
            ['background'],
        ];
    }

    /**
     * Test the job behaves as expected
     * @group ensure-indexes
     */
    public function testSuccessfullyEnsureIndexesJob()
    {
        $job = $this->createMockJob();
        $job->args = $this->createDefaultArguments();
        $this->jobSuccessfullyEnsuresIndexes($job);

        $this->performJob($job);
    }

    /**
     * Test that the job fails by throwing an exception
     * @group ensure-indexes
     */
    public function testEnsureIndexesJobThrowsErrorWhenCreatingIndexes()
    {
        $job = $this->createMockJob();
        $job->args = $this->createDefaultArguments();
        $this->jobThrowsExceptionWhenEnsuringIndexes($job);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Ensuring index failed');

        $this->performJob($job);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will use the default queue name
     */
    public function testEnsureIndexesCreateJobDefaultQueue()
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Tripod\Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        // create mock job
        $job = $this->createMockJob();
        $job->expects($this->once())
            ->method('submitJob')
            ->with(
                Tripod\Mongo\Config::getEnsureIndexesQueueName(),
                'MockEnsureIndexes',
                $jobData
            );

        $job->createJob('tripod_php_testing', false, true);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will throw the expected exception if redis is unreachable
     */
    public function testEnsureIndexesCreateJobUnreachableRedis()
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Tripod\Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        // create mock job
        $job = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods(['warningLog', 'enqueue'])
            ->getMock();

        $e = new Exception('Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known');

        // this is called 6 times because after the first attempt fails it will
        // retry 5 times.
        $job->expects($this->exactly(6))->method('enqueue')->will($this->throwException($e));

        // expect 5 retries. Catch this with call to warning log
        $job->expects($this->exactly(5))->method('warningLog');

        $this->expectException(Tripod\Exceptions\JobException::class);
        $this->expectExceptionMessage('Exception queuing job  - Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known');
        $job->createJob('tripod_php_testing', false, true);

    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will throw the expected exception if the job fails
     */
    public function testEnsureIndexesCreateJobStatusFalse()
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Tripod\Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        $job = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods(['warningLog', 'enqueue', 'getJobStatus'])
            ->getMock();

        // both of these methods will be called 6 times because after the first attempt fails it will
        // retry 5 times.
        $job->expects($this->exactly(6))->method('enqueue')->will($this->returnValue('sometoken'));
        $job->expects($this->exactly(6))->method('getJobStatus')->will($this->returnValue(false));

        // expect 5 retries. Catch this with call to warning log
        $job->expects($this->exactly(5))->method('warningLog');
        $this->expectException(Tripod\Exceptions\JobException::class);
        $this->expectExceptionMessage('Exception queuing job  - Could not retrieve status for queued job - job sometoken failed to tripod::ensureindexes');
        $job->createJob('tripod_php_testing', false, true);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will use the queue name specified
     */
    public function testEnsureIndexesCreateJobSpecifyQueue()
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Tripod\Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        $job = $this->createMockJob();

        $queueName = Tripod\Mongo\Config::getEnsureIndexesQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        $job->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockEnsureIndexes',
                $jobData
            );

        $job->createJob('tripod_php_testing', false, true, $queueName);
    }
    // HELPER METHODS BELOW HERE

    /**
     *  Creates a simple mock EnsureIndexes Job
     *
     *  @param  array list of methods to stub
     *  @return MockObject&\Tripod\Mongo\Jobs\EnsureIndexes
     */
    protected function createMockJob($methods = [])
    {
        $methodsToStub = ['getIndexUtils', 'submitJob', 'warningLog', 'enqueue', 'getJobStatus'];

        if (!empty($methods)) {
            $methodsToStub = $methods;
        }

        $mockEnsureIndexesJob = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods($methodsToStub)
            ->setMockClassName('MockEnsureIndexes')
            ->getMock();
        $mockEnsureIndexesJob->job = new Resque_Job('queue', ['id' => uniqid()]);
        return $mockEnsureIndexesJob;
    }

    /**
     * Returns default arguments for a EnsureIndexes Job
     * @return array
     */
    protected function createDefaultArguments()
    {
        return [
            'tripodConfig' => Tripod\Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'reindex' => false,
            'background' => true,
        ];
    }

    /**
     * @param MockObject&EnsureIndexes $job EnsureIndexes Job
     */
    protected function jobSuccessfullyEnsuresIndexes($job)
    {
        $mockIndexUtils = $this->getMockBuilder(Tripod\Mongo\IndexUtils::class)
            ->onlyMethods(['ensureIndexes'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true);

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->will($this->returnValue($mockIndexUtils));
    }

    /**
     * @param MockObject&EnsureIndexes $job EnsureIndexes Job
     */
    protected function jobThrowsExceptionWhenEnsuringIndexes($job)
    {
        $mockIndexUtils = $this->getMockBuilder(Tripod\Mongo\IndexUtils::class)
            ->onlyMethods(['ensureIndexes'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true)
            ->will($this->throwException(new Exception('Ensuring index failed')));

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->will($this->returnValue($mockIndexUtils));

    }
}
