<?php

declare(strict_types=1);

use Tripod\Mongo\Jobs\JobBase;

class ResqueJobTestBase extends MongoTripodTestBase
{
    protected function performJob(JobBase $job)
    {
        $mockJob = $this->getMockBuilder(Resque_Job::class)
            ->onlyMethods(['getInstance', 'getArguments'])
            ->setConstructorArgs(['test', get_class($job), $job->args])
            ->getMock();
        $mockJob->expects($this->atLeastOnce())
            ->method('getInstance')
            ->willReturn($job);
        $mockJob
            ->method('getArguments')
            ->willReturn($job->args);
        $mockJob->perform();
    }
}
