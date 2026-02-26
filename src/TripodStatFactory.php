<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Mongo\NoStat;

class TripodStatFactory
{
    /**
     * @return ITripodStat
     * @param array<string, mixed> $config
     */
    public static function create(array $config = [])
    {
        if (isset($config['class']) && class_exists($config['class'])) {
            return call_user_func([$config['class'], 'createFromConfig'], $config);
        }

        return NoStat::createFromConfig($config);
    }
}
