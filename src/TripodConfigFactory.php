<?php

namespace Tripod;

use Tripod\Mongo\IConfigInstance;

class TripodConfigFactory
{
    /**
     * Factory method to get a Tripod config instance from either a config array, or a serialized
     * ITripodConfigSerializer instance.
     *
     * @param array $config The Tripod config or serialized ITripodConfigSerializer array
     *
     * @return IConfigInstance
     */
    public static function create(array $config)
    {
        if (Config::getConfig() !== $config) {
            Config::setConfig($config);
        }
        if (isset($config['class']) && class_exists($config['class'])) {
            $tripodConfig = call_user_func([$config['class'], 'deserialize'], $config);
        } else {
            $tripodConfig = Mongo\Config::deserialize($config);
        }

        return $tripodConfig;
    }
}
