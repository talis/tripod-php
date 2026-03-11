<?php

declare(strict_types=1);

use Xhgui\Profiler\Profiler;

abstract class MongoTripodPerformanceTestBase extends MongoTripodTestBase
{
    /** @var Profiler|null */
    private $profiler;

    /**
     * Helper function to calculate difference between two microtime values.
     *
     * @param string $microTime1, time string from microtime()
     * @param string $microTime2, time string from microtime()
     *
     * @return float time difference in ms
     */
    protected function getTimeDifference(string $microTime1, string $microTime2): float
    {
        [$endTimeMicroSeconds, $endTimeSeconds] = explode(' ', $microTime2);
        [$startTimeMicroSeconds, $startTimeSeconds] = explode(' ', $microTime1);

        $differenceInMilliSeconds = ((float) $endTimeSeconds - (float) $startTimeSeconds) * 1000;

        return round(($differenceInMilliSeconds + ((float) $endTimeMicroSeconds * 1000)) - (float) $startTimeMicroSeconds * 1000);
    }

    protected function startProfiler(): void
    {
        $this->profiler = $this->getProfiler();
        $this->profiler->start();
    }

    /**
     * @after
     */
    protected function stopProfiler(): void
    {
        if ($this->profiler) {
            $this->profiler->stop();
            $this->profiler = null;
        }
    }

    private function getProfiler(): Profiler
    {
        $profilerDir = __DIR__ . '/../../../profiler';
        if (!is_dir($profilerDir)) {
            mkdir($profilerDir, 0777, true);
        }

        return new Profiler([
            'save.handler' => Profiler::SAVER_FILE,
            'save.handler.file' => [
                'filename' => $profilerDir . '/xhgui.data.jsonl',
            ],
            'profiler.replace_url' => function (): string {
                return $this->getName(true) . ($this->hasFailed() ? ' FAIL' : '');
            },
        ]);
    }
}
