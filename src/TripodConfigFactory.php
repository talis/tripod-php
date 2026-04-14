<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Mongo\IConfigInstance;

class TripodConfigFactory
{
    /**
     * Factory method to get a Tripod config instance from either a config array, or a serialized
     * ITripodConfigSerializer instance.
     *
     * @param array<string, mixed> $config The Tripod config or serialized ITripodConfigSerializer array
     */
    public static function create(array $config): IConfigInstance
    {
        if (Config::getConfig() !== $config) {
            Config::setConfig($config);
        }

        if (isset($config['class']) && class_exists($config['class'])) {
            return call_user_func([$config['class'], 'deserialize'], $config);
        }

        return Mongo\Config::deserialize($config);
    }
}
