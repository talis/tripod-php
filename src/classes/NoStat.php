<?php

declare(strict_types=1);

namespace Tripod;

final class NoStat implements ITripodStat
{
    /**
     * @var self
     */
    public static $instance;

    public function increment(string $operation, int $inc = 1): void
    {
        // do nothing
    }

    /**
     * @param float|int $duration
     */
    public function timer(string $operation, $duration): void
    {
        // do nothing
    }

    public function getConfig(): array
    {
        return [];
    }

    public static function getInstance(): self
    {
        if (self::$instance == null) {
            self::$instance = new NoStat();
        }

        return self::$instance;
    }

    /**
     * @return self
     */
    public static function createFromConfig(array $config = [])
    {
        return self::getInstance();
    }
}
