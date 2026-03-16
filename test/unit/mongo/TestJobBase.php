<?php

use Tripod\Mongo\IConfigInstance;
use Tripod\Mongo\Jobs\JobBase;

class TestJobBase extends JobBase
{
    /**
     * Expose this method for testing.
     * {@inheritDoc}
     */
    public function getTripodConfig(): IConfigInstance
    {
        return parent::getTripodConfig();
    }

    public function perform() {}

    protected function getStatTimerSuccessKey(): string
    {
        return 'TEST_SUCCESS';
    }

    protected function getStatFailureIncrementKey(): string
    {
        return 'TEST_FAIL';
    }
}
