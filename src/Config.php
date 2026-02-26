<?php

namespace Tripod;

use Tripod\Exceptions\ConfigException;
use Tripod\Mongo\IConfigInstance;

class Config implements ITripodConfig
{
    /**
     * @var IConfigInstance|null
     */
    private static $instance;

    /**
     * @var array
     */
    private static $config = [];

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
        if (!isset(self::$instance)) {
            self::$instance = TripodConfigFactory::create(self::$config);
        }

        return self::$instance;
    }

    /**
     * Loads the Tripod config into the instance.
     */
    public static function setConfig(array $config): void
    {
        self::$config = $config;
        self::$instance = null; // this will force a reload next time getInstance() is called
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
        self::$instance = null;
        self::$config = null;
    }
}
