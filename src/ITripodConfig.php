<?php

namespace Tripod;

use Tripod\Mongo\IConfigInstance;

interface ITripodConfig
{
    /**
     * Tripod Config instances are singletons, this method gets the existing or instantiates a new one.
     */
    public static function getInstance(): IConfigInstance;

    /**
     * Loads the Tripod config into the instance
     */
    public static function setConfig(array $config): void;

    /**
     * Returns the Tripod config array
     */
    public static function getConfig(): array;
}
