<?php

declare(strict_types=1);

namespace Tripod\Mongo\Jobs;

use Tripod\Mongo\IndexUtils;

class EnsureIndexes extends JobBase
{
    public const STORENAME_KEY = 'storeName';

    public const REINDEX_KEY = 'reindex';

    public const BACKGROUND_KEY = 'background';

    protected $mandatoryArgs = [self::STORENAME_KEY, self::REINDEX_KEY, self::BACKGROUND_KEY];

    protected $configRequired = true;

    /**
     * Runs the EnsureIndexes Job.
     *
     * @throws \Exception
     */
    public function perform(): void
    {
        $this->debugLog('Ensuring indexes for tenant=' . $this->args[self::STORENAME_KEY] . ', reindex=' . $this->args[self::REINDEX_KEY] . ', background=' . $this->args[self::BACKGROUND_KEY]);

        $this->getIndexUtils()->ensureIndexes(
            $this->args[self::REINDEX_KEY],
            $this->args[self::STORENAME_KEY],
            $this->args[self::BACKGROUND_KEY]
        );
    }

    /**
     * This method is use to schedule an EnsureIndexes job.
     *
     * @param string  $storeName
     * @param booelan $reindex
     * @param string  $queueName
     * @param mixed   $background
     */
    public function createJob($storeName, $reindex, $background, $queueName = null): void
    {
        $configInstance = $this->getConfigInstance();
        if (!$queueName) {
            $queueName = $configInstance::getEnsureIndexesQueueName();
        } elseif (strpos($queueName, $configInstance::getEnsureIndexesQueueName()) === false) {
            $queueName = $configInstance::getEnsureIndexesQueueName() . '::' . $queueName;
        }

        $data = [
            self::STORENAME_KEY => $storeName,
            self::REINDEX_KEY => $reindex,
            self::BACKGROUND_KEY => $background,
        ];

        $this->submitJob($queueName, get_class($this), array_merge($data, $this->generateConfigJobArgs()));
    }

    /**
     * Stat string for successful job timer.
     */
    protected function getStatTimerSuccessKey(): string
    {
        return MONGO_QUEUE_ENSURE_INDEXES_SUCCESS;
    }

    /**
     * Stat string for failed job increment.
     */
    protected function getStatFailureIncrementKey(): string
    {
        return MONGO_QUEUE_ENSURE_INDEXES_FAIL;
    }

    protected function getIndexUtils(): \Tripod\Mongo\IndexUtils
    {
        return new IndexUtils();
    }
}
