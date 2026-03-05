<?php

declare(strict_types=1);

abstract class MongoTripodPerformanceTestBase extends MongoTripodTestBase
{
    /**
     * Helper function to calculate difference between two microtime values.
     *
     * @param $microTime1, time string from microtime()
     * @param $microTime2, time string from microtime()
     *
     * @return float, time difference in ms
     */
    protected function getTimeDifference($microTime1, $microTime2)
    {
        [$endTimeMicroSeconds, $endTimeSeconds] = explode(' ', $microTime2);
        [$startTimeMicroSeconds, $startTimeSeconds] = explode(' ', $microTime1);

        $differenceInMilliSeconds = ((float) $endTimeSeconds - (float) $startTimeSeconds) * 1000;

        return round(($differenceInMilliSeconds + ((float) $endTimeMicroSeconds * 1000)) - (float) $startTimeMicroSeconds * 1000);
    }
}
