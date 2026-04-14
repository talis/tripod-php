<?php

declare(strict_types=1);

namespace Tripod;

class TripodStatFactory
{
    /**
     * @param array<string, mixed> $config
     *
     * @return ITripodStat
     */
    public static function create(array $config = [])
    {
        if (isset($config['class']) && class_exists($config['class'])) {
            return call_user_func([$config['class'], 'createFromConfig'], $config);
        }

        return NoStat::createFromConfig($config);
    }
}
