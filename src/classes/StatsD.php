<?php

declare(strict_types=1);

namespace Tripod;

class StatsD implements ITripodStat
{
    /** @var string */
    private $host;

    /** @var int|string */
    private $port;

    /** @var string */
    private $prefix;

    /** @var string */
    private $pivotValue;

    /**
     * @param string     $host
     * @param int|string $port
     * @param string     $prefix
     */
    public function __construct($host, $port, $prefix = '')
    {
        $this->host = $host;
        $this->port = $port;
        $this->setPrefix($prefix);
    }

    /**
     * @param string $operation
     * @param int    $inc
     *
     * @return void
     */
    public function increment($operation, $inc = 1)
    {
        $this->send(
            $this->generateStatData($operation, $inc . '|c')
        );
    }

    /**
     * @param string $operation
     * @param number $duration
     *
     * @return void
     */
    public function timer($operation, $duration)
    {
        $this->send(
            $this->generateStatData($operation, ['1|c', $duration . '|ms'])
        );
    }

    /**
     * Record an arbitrary value.
     *
     * @param string $operation
     */
    public function gauge($operation, string $value): void
    {
        $this->send(
            $this->generateStatData($operation, $value . '|g')
        );
    }

    /**
     * @return array<string, array<string, int|string>|class-string<StatsD>>
     */
    public function getConfig()
    {
        return [
            'class' => get_class($this),
            'config' => [
                'host' => $this->host,
                'port' => $this->port,
                'prefix' => $this->prefix,
            ],
        ];
    }

    public static function createFromConfig(array $config)
    {
        if (isset($config['config'])) {
            $config = $config['config'];
        }

        $host = ($config['host'] ?? null);
        $port = ($config['port'] ?? null);
        $prefix = ($config['prefix'] ?? '');

        return new self($host, $port, $prefix);
    }

    /**
     * @return string
     */
    public function getPrefix()
    {
        return $this->prefix;
    }

    /**
     * @param string $prefix
     *
     * @throws \InvalidArgumentException
     */
    public function setPrefix($prefix): void
    {
        if ($this->isValidPathValue($prefix)) {
            $this->prefix = $prefix;
        } else {
            throw new \InvalidArgumentException('Invalid prefix supplied');
        }
    }

    /**
     * @return int|string
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param int|string $port
     */
    public function setPort($port): void
    {
        $this->port = $port;
    }

    /**
     * @return string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param string $host
     */
    public function setHost($host): void
    {
        $this->host = $host;
    }

    /**
     * @return string
     */
    public function getPivotValue()
    {
        return $this->pivotValue;
    }

    /**
     * @param string $pivotValue
     *
     * @throws \InvalidArgumentException
     */
    public function setPivotValue($pivotValue): void
    {
        if ($this->isValidPathValue($pivotValue)) {
            $this->pivotValue = $pivotValue;
        } else {
            throw new \InvalidArgumentException('Invalid pivot value supplied');
        }
    }

    /**
     * Sends the stat(s) using UDP protocol.
     *
     * @param array $data
     * @param int   $sampleRate
     *
     * @return void
     */
    protected function send($data, $sampleRate = 1)
    {
        if (empty($this->host)) {
            return;
        }

        $sampledData = [];
        if ($sampleRate < 1) {
            foreach ($data as $stat => $value) {
                if ((mt_rand() / mt_getrandmax()) <= $sampleRate) {
                    $sampledData[$stat] = sprintf('%s|@%d', $value, $sampleRate);
                }
            }
        } else {
            $sampledData = $data;
        }

        if (empty($sampledData)) {
            return;
        }

        try {
            $fp = fsockopen('udp://' . $this->host, $this->port);
            if (!$fp) {
                return;
            }

            // make this a non blocking stream
            stream_set_blocking($fp, false);
            foreach ($sampledData as $stat => $value) {
                if (is_array($value)) {
                    foreach ($value as $v) {
                        fwrite($fp, sprintf('%s:%s', $stat, $v));
                    }
                } else {
                    fwrite($fp, sprintf('%s:%s', $stat, $value));
                }
            }

            fclose($fp);
        } catch (\Exception $e) {
        }
    }

    /**
     * This method combines the by database and aggregate stats to send to StatsD.  The return will look something list:
     * {
     *  "{prefix}.tripod.group_by_db.{storeName}.{stat}"=>"1|c",
     *  "{prefix}.tripod.{stat}"=>"1|c"
     * }
     *
     * @param array|string $value
     *
     * @return array An associative array of the grouped_by_database and aggregate stats
     */
    protected function generateStatData(string $operation, $value): array
    {
        $data = [];
        foreach ($this->getStatsPaths() as $path) {
            $data[$path . ('.' . $operation)] = $value;
        }

        return $data;
    }

    protected function getStatsPaths(): array
    {
        return array_values(array_filter([$this->getAggregateStatPath()]));
    }

    protected function getAggregateStatPath(): string
    {
        return empty($this->prefix) ? STAT_CLASS : $this->prefix . '.' . STAT_CLASS;
    }

    /**
     * StatsD paths cannot start with, end with, or have more than one consecutive '.'.
     *
     * @param string $value
     */
    protected function isValidPathValue($value): bool
    {
        return preg_match('/(^\.)|(\.\.+)|(\.$)/', $value) === 0;
    }
}
