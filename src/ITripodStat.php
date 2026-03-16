<?php

declare(strict_types=1);

namespace Tripod;

interface ITripodStat
{
    /**
     * @param int $inc Amount to increment by
     */
    public function increment(string $operation, int $inc = 1): void;

    /**
     * @param float|int $duration
     */
    public function timer(string $operation, $duration): void;

    public function getConfig(): array;

    /**
     * @return self
     */
    public static function createFromConfig(array $config);
}
