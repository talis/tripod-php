<?php

declare(strict_types=1);

namespace Tripod;

use Psr\Log\LogLevel;

/**
 * Common logging methods for Tripod classes.
 * Classes that use this trait must implement a static getLogger() method.
 */
trait LoggerTrait
{
    /**
     * @codeCoverageIgnore
     */
    public function timingLog(string $type, ?array $params = null): void
    {
        $type = '[PID ' . getmypid() . '] ' . $type;
        $this->log(LogLevel::DEBUG, $type, $params);
    }

    /**
     * @codeCoverageIgnore
     */
    public function infoLog(string $message, ?array $params = null): void
    {
        $message = '[PID ' . getmypid() . '] ' . $message;
        $this->log(LogLevel::INFO, $message, $params);
    }

    /**
     * @codeCoverageIgnore
     */
    public function debugLog(string $message, ?array $params = null): void
    {
        $message = '[PID ' . getmypid() . '] ' . $message;
        $this->log(LogLevel::DEBUG, $message, $params);
    }

    /**
     * @codeCoverageIgnore
     */
    public function errorLog(string $message, ?array $params = null): void
    {
        $message = '[PID ' . getmypid() . '] ' . $message;
        $this->log(LogLevel::ERROR, $message, $params);
    }

    /**
     * @codeCoverageIgnore
     */
    public function warningLog(string $message, ?array $params = null): void
    {
        $message = '[PID ' . getmypid() . '] ' . $message;
        $this->log(LogLevel::WARNING, $message, $params);
    }

    /**
     * @codeCoverageIgnore
     */
    private function log(string $level, string $message, ?array $params): void
    {
        self::getLogger()->log($level, $message, $params ?: []);
    }
}
