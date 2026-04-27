<?php

// Classes provided here allow Tripod to work with either version of php-resque:
// - <=1.3.6 may cause problems on PHP 8 due to the use of dynamic properties.
// - later versions introduce namespaces and jobs must extend Resque\Job\Job.

// @codeCoverageIgnoreStart

namespace Resque {
    if (!class_exists(Resque::class) && class_exists(\Resque::class)) {
        class Resque extends \Resque {}
    }

    if (!class_exists(Event::class) && class_exists(\Resque_Event::class)) {
        class Event extends \Resque_Event {}
    }

    if (!class_exists(JobHandler::class) && class_exists(\Resque_Job::class)) {
        class JobHandler extends \Resque_Job {}
    }
}

namespace Resque\Job {
    use Resque\JobHandler;

    if (!class_exists(Status::class) && class_exists(\Resque_Job_Status::class)) {
        class Status extends \Resque_Job_Status {}
    }

    if (!class_exists(Job::class)) {
        abstract class Job
        {
            /**
             * Job arguments.
             *
             * @var array
             */
            public $args;

            /**
             * Associated JobHandler instance.
             *
             * @var JobHandler
             */
            public $job;

            /**
             * Name of the queue the job was in.
             *
             * @var string
             */
            public $queue;

            /**
             * Unique job ID.
             *
             * @var string
             */
            public $jobID;

            /**
             * (Optional) Job setup.
             */
            public function setUp(): void
            {
                // no-op
            }

            /**
             * (Optional) Job teardown.
             */
            public function tearDown(): void
            {
                // no-op
            }

            /**
             * Main method of the Job.
             *
             * @return mixed|void
             */
            abstract public function perform();
        }
    }
}
// @codeCoverageIgnoreEnd
