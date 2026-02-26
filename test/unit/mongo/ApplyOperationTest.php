<?php

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use Tripod\Config;
use Tripod\Exceptions\JobException;
use Tripod\Mongo\Composites\SearchIndexer;
use Tripod\Mongo\Composites\Tables;
use Tripod\Mongo\Composites\Views;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\JobGroup;
use Tripod\Mongo\Jobs\ApplyOperation;
use Tripod\Mongo\MongoSearchProvider;

class ApplyOperationTest extends ResqueJobTestBase
{
    protected $args = [];

    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new ApplyOperation();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument tripodConfig or tripodConfigGenerator was not present in supplied job args for job Tripod\Mongo\Jobs\ApplyOperation');
        $this->performJob($job);
    }

    public function testMandatoryArgSubject()
    {
        $this->setArgs();
        unset($this->args['subjects']);
        $job = new ApplyOperation();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument subjects was not present in supplied job args for job Tripod\Mongo\Jobs\ApplyOperation');
        $this->performJob($job);
    }

    public function testApplyViewOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat'])
            ->getMock();

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_VIEWS,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Views::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisapire.com/',
            ])->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_VIEWS, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($views));

        $views->expects($this->once())
            ->method('update')
            ->with($subject);

        $this->performJob($applyOperation);
    }

    public function testApplyViewOperationDecrementsJobGroupForBatchOperations()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod'])
            ->getMock();

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);
        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(2));

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_VIEWS,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Views::class)
            ->onlyMethods(['update', 'deleteViewsByViewId'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisapire.com/',
                ]
            )->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->never())
            ->method('getTripod');

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_VIEWS, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($views));

        $views->expects($this->once())
            ->method('update')
            ->with($subject);

        $views->expects($this->never())->method('deleteViewsByViewId');
        $this->performJob($applyOperation);
    }

    public function testApplyViewOperationCleanupIfAllGroupJobsComplete()
    {
        $this->setArgs(OP_VIEWS, ['v_foo_bar']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod'])
            ->getMock();

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);
        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();
        $timestamp = new UTCDateTime(hexdec(substr($jobTrackerId, 0, 8)) * 1000);

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(0));

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_VIEWS,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Views::class)
            ->onlyMethods(['update', 'deleteViewsByViewId'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisapire.com/',
                ]
            )->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->once())
            ->method('getTripod')
            ->with('tripod_php_testing', 'CBD_testing')
            ->will($this->returnValue($tripod));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_VIEWS, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->exactly(2))
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($views));

        $views->expects($this->once())
            ->method('update')
            ->with($subject);

        $views->expects($this->once())
            ->method('deleteViewsByViewId')
            ->with('v_foo_bar', $timestamp)
            ->will($this->returnValue(3));

        $this->performJob($applyOperation);
    }

    public function testApplyTableOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $impactedSubject = new ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo',
                _ID_CONTEXT => 'http://talisaspire.com/',
            ],
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            ['t_resource']
        );
        $this->args['subjects'] = [$impactedSubject->toArray()];

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_TABLES,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $tables = $this->getMockBuilder(Tables::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisapire.com/',
            ])->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_TABLES, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $tables->expects($this->once())
            ->method('update')
            ->with($subject);

        $this->performJob($applyOperation);
    }

    public function testApplyTableOperationDecrementsJobGroupForBatchOperations()
    {
        $this->setArgs(OP_TABLES, ['t_resource']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(2));

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_TABLES,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $tables = $this->getMockBuilder(Tables::class)
            ->onlyMethods(['update', 'deleteTableRowsByTableId'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisapire.com/',
                ]
            )->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->never())
            ->method('getTripod');

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_TABLES, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $tables->expects($this->once())
            ->method('update')
            ->with($subject);
        $tables->expects($this->never())
            ->method('deleteTableRowsByTableId');

        $this->performJob($applyOperation);
    }

    public function testApplyTableOperationCleanupIfAllGroupJobsComplete()
    {
        $this->setArgs(OP_TABLES, ['t_resource']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();
        $timestamp = new UTCDateTime(hexdec(substr($jobTrackerId, 0, 8)) * 1000);

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(0));

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_TABLES,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $tables = $this->getMockBuilder(Tables::class)
            ->onlyMethods(['update', 'deleteTableRowsByTableId'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisapire.com/',
                ]
            )->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->once())
            ->method('getTripod')
            ->with('tripod_php_testing', 'CBD_testing')
            ->will($this->returnValue($tripod));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_TABLES, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->exactly(2))
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $tables->expects($this->once())
            ->method('update')
            ->with($subject);
        $tables->expects($this->once())
            ->method('deleteTableRowsByTableId')
            ->with('t_resource', $timestamp)
            ->will($this->returnValue(4));

        $this->performJob($applyOperation);
    }

    public function testApplySearchOperation()
    {
        $this->setArgs(OP_SEARCH, ['i_search_resource']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $search = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_SEARCH, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_SEARCH)
            ->will($this->returnValue($search));

        $search->expects($this->once())
            ->method('update')
            ->with($subject);

        $this->performJob($applyOperation);
    }

    public function testApplySearchOperationDecrementsJobGroupForBatchOperations()
    {
        $this->setArgs(OP_SEARCH, ['i_search_resource']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(2));

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $search = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->never())
            ->method('getTripod');

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_SEARCH, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_SEARCH)
            ->will($this->returnValue($search));

        $search->expects($this->once())
            ->method('update')
            ->with($subject);

        $this->performJob($applyOperation);
    }

    public function testApplySearchOperationCleanupIfAllGroupJobsComplete()
    {
        $this->setArgs(OP_SEARCH, ['i_search_resource']);
        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['createImpactedSubject', 'getStat', 'getJobGroup', 'getTripod', 'getSearchProvider'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $applyOperation->args = $this->args;
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $jobTrackerId = new ObjectId();
        $applyOperation->args[ApplyOperation::TRACKING_KEY] = $jobTrackerId->__toString();
        $timestamp = new UTCDateTime(hexdec(substr($jobTrackerId, 0, 8)) * 1000);

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['incrementJobCount'])
            ->setConstructorArgs(['tripod_php_testing', $jobTrackerId])
            ->getMock();

        $jobGroup->expects($this->once())
            ->method('incrementJobCount')
            ->with(-1)
            ->will($this->returnValue(0));

        $subject = $this->getMockBuilder(ImpactedSubject::class)
            ->onlyMethods(['getTripod'])
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://example.com/resources/foo',
                        _ID_CONTEXT => 'http://talisaspire.com',
                    ],
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )->getMock();

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $searchProvider = $this->getMockBuilder(MongoSearchProvider::class)
            ->onlyMethods(['deleteSearchDocumentsByTypeId'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $search = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->once())
            ->method('getJobGroup')
            ->with('tripod_php_testing', $jobTrackerId->__toString())
            ->will($this->returnValue($jobGroup));

        $applyOperation->expects($this->once())
            ->method('getTripod')
            ->with('tripod_php_testing', 'CBD_testing')
            ->will($this->returnValue($tripod));

        $applyOperation->expects($this->once())
            ->method('getSearchProvider')
            ->with($tripod)
            ->will($this->returnValue($searchProvider));

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, 1);

        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_APPLY_OPERATION . '.' . OP_SEARCH, $this->anything()],
                [MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything()]
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_SEARCH)
            ->will($this->returnValue($search));

        $search->expects($this->once())
            ->method('update')
            ->with($subject);

        $searchProvider->expects($this->once())
            ->method('deleteSearchDocumentsByTypeId')
            ->with('i_search_resource', $timestamp)
            ->will($this->returnValue(8));

        $this->performJob($applyOperation);
    }

    public function testCreateJobDefaultQueue()
    {
        $impactedSubject = new ImpactedSubject(
            [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            ['t_resource', 't_resource_count']
        );

        $jobData = [
            'subjects' => [$impactedSubject->toArray()],
            'tripodConfig' => Config::getConfig(),
        ];

        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['submitJob'])
            ->setMockClassName('MockApplyOperation')
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('submitJob')
            ->with(
                Tripod\Mongo\Config::getApplyQueueName(),
                'MockApplyOperation',
                $jobData
            );

        $applyOperation->createJob([$impactedSubject]);
    }

    public function testCreateJobUnreachableRedis()
    {
        $impactedSubject = new ImpactedSubject(
            [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            ['t_resource', 't_resource_count']
        );

        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['warningLog', 'enqueue'])
            ->getMock();

        $e = new Exception('Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known');
        $applyOperation->expects($this->any())->method('enqueue')->will($this->throwException($e));

        // expect 5 retries. Catch this with call to warning log
        $applyOperation->expects($this->exactly(5))->method('warningLog');

        $exceptionThrown = false;

        try {
            $applyOperation->createJob([$impactedSubject]);
        } catch (JobException $e) {
            $this->assertEquals('Exception queuing job  - Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known', $e->getMessage());
            $exceptionThrown = true;
        }
        if (!$exceptionThrown) {
            $this->fail('Did not throw JobException');
        }
    }

    public function testCreateJobStatusFalse()
    {
        $impactedSubject = new ImpactedSubject(
            [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            ['t_resource', 't_resource_count']
        );

        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['enqueue', 'getJobStatus', 'warningLog'])
            ->getMock();

        $applyOperation->expects($this->any())->method('enqueue')->will($this->returnValue('sometoken'));
        $applyOperation->expects($this->any())->method('getJobStatus')->will($this->returnValue(false));

        // expect 5 retries. Catch this with call to warning log
        $applyOperation->expects($this->exactly(5))->method('warningLog');

        $exceptionThrown = false;

        try {
            $applyOperation->createJob([$impactedSubject]);
        } catch (JobException $e) {
            $this->assertEquals('Exception queuing job  - Could not retrieve status for queued job - job sometoken failed to tripod::apply', $e->getMessage());
            $exceptionThrown = true;
        }
        if (!$exceptionThrown) {
            $this->fail('Did not throw JobException');
        }
    }

    public function testCreateJobSpecifyQueue()
    {
        $impactedSubject = new ImpactedSubject(
            [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
            OP_VIEWS,
            'tripod_php_testing',
            'CBD_testing',
            []
        );

        $jobData = [
            'subjects' => [$impactedSubject->toArray()],
            'tripodConfig' => Config::getConfig(),
        ];

        $applyOperation = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['submitJob'])
            ->setMockClassName('MockApplyOperation')
            ->getMock();

        $queueName = Tripod\Mongo\Config::getApplyQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        $applyOperation->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockApplyOperation',
                $jobData
            );

        $applyOperation->createJob([$impactedSubject], $queueName);
    }

    /**
     * Sets job arguments.
     *
     * @param mixed $operation
     */
    protected function setArgs($operation = OP_VIEWS, array $specTypes = [])
    {
        $subject = new ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo',
                _ID_CONTEXT => 'http://talisaspire.com/',
            ],
            $operation,
            'tripod_php_testing',
            'CBD_testing',
            $specTypes
        );

        $this->args = [
            'tripodConfig' => Config::getConfig(),
            'subjects' => [$subject->toArray()],
            'statsConfig' => $this->getStatsDConfig(),
        ];
    }
}
