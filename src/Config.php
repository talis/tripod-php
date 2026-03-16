<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Exceptions\ConfigException;
use Tripod\Mongo\IConfigInstance;

class Config implements ITripodConfig
{
    private static ?IConfigInstance $configInstance = null;

    private static ?array $config = null;

    /**
     * Config should not be instantiated directly: use Config::getInstance().
     */
    private function __construct() {}

    /**
     * Since this is a singleton class, use this method to create a new config instance.
     *
     * @uses               Config::setConfig() Configuration must be set prior to calling this method. To generate a completely new object, set a new config
     *
     * @codeCoverageIgnore
     *
     * @throws ConfigException
     */
    public static function getInstance(): IConfigInstance
    {
        if (!isset(self::$config)) {
            throw new ConfigException('Call Config::setConfig() first');
        }

        if (!isset(self::$configInstance)) {
            self::$configInstance = TripodConfigFactory::create(self::$config);
        }

        return self::$configInstance;
    }

    /**
     * Loads the Tripod config into the instance.
     */
    public static function setConfig(array $config): void
    {
        self::$config = $config;
        self::$configInstance = null; // this will force a reload next time getInstance() is called
    }

    /**
     * Returns the Tripod config array.
     */
    public static function getConfig(): array
    {
        return self::$config;
    }

    /**
     * This method was added to allow us to test the getInstance() method.
     *
     * @codeCoverageIgnore
     */
    public static function destroy(): void
    {
        self::$configInstance = null;
        self::$config = null;
    }
}
