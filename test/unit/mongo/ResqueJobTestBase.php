<?php

declare(strict_types=1);

use Resque\JobHandler;
use Tripod\Mongo\Jobs\JobBase;

class ResqueJobTestBase extends MongoTripodTestBase
{
    protected function performJob(JobBase $job): void
    {
        $mockJob = $this->getMockBuilder(JobHandler::class)
            ->onlyMethods(['getInstance', 'getArguments'])
            ->setConstructorArgs(['test', [
                'class' => get_class($job),
                'args' => [$job->args],
            ]])
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
