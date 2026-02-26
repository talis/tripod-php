<?php

use Tripod\Mongo\Jobs\JobBase;

class TestJobBase extends JobBase
{
    /**
     * Expose this method for testing.
     * {@inheritDoc}
     */
    public function getTripodConfig()
    {
        return parent::getTripodConfig();
    }

    public function perform() {}

    protected function getStatTimerSuccessKey()
    {
        return 'TEST_SUCCESS';
    }

    protected function getStatFailureIncrementKey()
    {
        return 'TEST_FAIL';
    }
}
