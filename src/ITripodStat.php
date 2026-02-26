<?php

declare(strict_types=1);

namespace Tripod;

interface ITripodStat
{
    /**
     * @param string     $operation
     * @param int|number $inc       Amount to increment by
     *
     * @return void
     */
    public function increment($operation, $inc = 1);

    /**
     * @param string $operation
     * @param number $duration
     *
     * @return void
     */
    public function timer($operation, $duration);

    /**
     * @return array
     */
    public function getConfig();

    /**
     * @return ITripodStat
     */
    public static function createFromConfig(array $config);
}
