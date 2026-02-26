<?php

namespace Tripod\Mongo\Jobs;

use MongoDB\Driver\ReadPreference;
use Tripod\Config;
use Tripod\Exceptions\Exception;
use Tripod\Exceptions\JobException;
use Tripod\ITripodConfigSerializer;
use Tripod\ITripodStat;
use Tripod\Mongo\Driver;
use Tripod\Mongo\DriverBase;
use Tripod\Mongo\IConfigInstance;
use Tripod\Timer;

abstract class JobBase extends DriverBase
{
    public const TRIPOD_CONFIG_KEY = 'tripodConfig';
    public const TRIPOD_CONFIG_GENERATOR = 'tripodConfigGenerator';
    public const QUEUE_KEY = 'queue';

    /**
     * Resque Job arguments, set by Resque_Job_Factory.
     *
     * @var array
     */
    public $args;

    /**
     * Resque Job queue, set by Resque_Job_Factory.
     *
     * @var string
     */
    public $queue;

    /**
     * Resque Job.
     *
     * @var \Resque_Job
     */
    public $job;

    protected $mandatoryArgs = [];
    protected $configRequired = false;

    /** @var IConfigInstance */
    protected $tripodConfig;

    /** @var Timer */
    protected $timer;

    private $tripod;

    public function setUp()
    {
        $this->debugLog(
            '[JOBID ' . $this->job->payload['id'] . '] ' . get_class($this) . '::perform() start'
        );

        $this->timer = new Timer();
        $this->timer->start();

        $this->setStatsConfig();

        if ($this->configRequired) {
            $this->setTripodConfig();
        }
    }

    public function tearDown()
    {
        // stat time taken to process item, from time it was created (queued)
        $this->timer->stop();
        $this->debugLog(
            '[JOBID ' . $this->job->payload['id'] . '] ' . get_class($this)
            . "::perform() done in {$this->timer->result()}ms"
        );
        $this->getStat()->timer($this->getStatTimerSuccessKey(), $this->timer->result());
    }

    /**
     * The main method of the job.
     */
    abstract public function perform();

    /**
     * Called in every job prior to perform().
     *
     * @param \Resque_Job The queued job
     */
    public static function beforePerform(\Resque_Job $job)
    {
        $instance = $job->getInstance();
        if (!$instance instanceof self) {
            return;
        }

        $instance->validateArgs();
    }

    /**
     * Resque event when a job failures.
     *
     * @param \Exception|\Throwable $e   Exception or Error
     * @param \Resque_Job           $job The failed job
     */
    public static function onFailure($e, \Resque_Job $job)
    {
        $failedJob = $job->getInstance();
        if (!$failedJob instanceof self) {
            return;
        }

        $failedJob->errorLog('Caught exception in ' . static::class . ': ' . $e->getMessage());
        $failedJob->getStat()->increment($failedJob->getStatFailureIncrementKey());
    }

    /**
     * @param string $message Log message
     * @param mixed  $params  Log params
     */
    public function debugLog($message, $params = null)
    {
        parent::debugLog($message, $params);
    }

    /**
     * @param string $message Log message
     * @param mixed  $params  Log params
     */
    public function errorLog($message, $params = null)
    {
        parent::errorLog($message, $params);
    }

    /**
     * @return ITripodStat
     */
    public function getStat()
    {
        if (!isset($this->statsConfig)) {
            $this->getStatsConfig();
        }

        return parent::getStat();
    }

    /**
     * Gets the stats config for the job.
     *
     * @return array
     */
    public function getStatsConfig()
    {
        if (empty($this->statsConfig)) {
            $this->setStatsConfig();
        }

        return $this->statsConfig;
    }

    /**
     * Stat string for successful job timer.
     *
     * @return string
     */
    abstract protected function getStatTimerSuccessKey();

    /**
     * Stat string for failed job increment.
     *
     * @return string
     */
    abstract protected function getStatFailureIncrementKey();

    /**
     * For mocking.
     *
     * @param string $storeName
     * @param string $podName
     * @param array  $opts
     *
     * @return Driver
     */
    protected function getTripod($storeName, $podName, $opts = [])
    {
        $this->getTripodConfig();

        $opts = array_merge(
            $opts,
            [
                'stat' => $this->getStat(),
                'readPreference' => ReadPreference::RP_PRIMARY, // important: make sure we always read from the primary
            ]
        );
        if ($this->tripod == null) {
            $this->tripod = new Driver(
                $podName,
                $storeName,
                $opts
            );
        }

        return $this->tripod;
    }

    /**
     * Make sure each job considers how to validate its args.
     *
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return $this->mandatoryArgs;
    }

    /**
     * Validate the arguments for this job.
     *
     * @throws \Exception
     */
    protected function validateArgs()
    {
        $message = null;
        foreach ($this->getMandatoryArgs() as $arg) {
            if (!isset($this->args[$arg])) {
                $message = "Argument {$arg} was not present in supplied job args for job " . get_class($this);
                $this->errorLog($message);

                throw new \Exception($message);
            }
        }
        if ($this->configRequired) {
            $this->ensureConfig();
        }
    }

    protected function ensureConfig()
    {
        if (!isset($this->args[self::TRIPOD_CONFIG_KEY]) && !isset($this->args[self::TRIPOD_CONFIG_GENERATOR])) {
            $message = 'Argument ' . self::TRIPOD_CONFIG_KEY . ' or ' . self::TRIPOD_CONFIG_GENERATOR
                . ' was not present in supplied job args for job ' . get_class($this);
            $this->errorLog($message);

            throw new \Exception($message);
        }
    }

    /**
     * @param string $queueName     Queue name
     * @param string $class         Class name
     * @param array  $data          Job arguments
     * @param int    $retryAttempts If queue fails, retry x times before throwing an exception
     *
     * @return string A tracking token for the submitted job
     *
     * @throws JobException If there is a problem queuing the job
     */
    protected function submitJob($queueName, $class, array $data, $retryAttempts = 5)
    {
        // @see https://github.com/chrisboulton/php-resque/issues/228, when this PR is merged we can stop tracking the status in this way
        try {
            if (isset($data[self::TRIPOD_CONFIG_GENERATOR]) && $data[self::TRIPOD_CONFIG_GENERATOR]) {
                $data[self::TRIPOD_CONFIG_GENERATOR] = $this->serializeConfig($data[self::TRIPOD_CONFIG_GENERATOR]);
            }
            $token = $this->enqueue($queueName, $class, $data);
            if (!$this->getJobStatus($token)) {
                $this->errorLog("Could not retrieve status for queued {$class} job - job {$token} failed to {$queueName}");

                throw new \Exception("Could not retrieve status for queued job - job {$token} failed to {$queueName}");
            }
            $this->debugLog("Queued {$class} job with {$token} to {$queueName}");

            return $token;
        } catch (\Exception $e) {
            if ($retryAttempts > 0) {
                sleep(1); // back off for 1 sec
                $this->warningLog("Exception queuing {$class} job - {$e->getMessage()}, retrying {$retryAttempts} times");

                return $this->submitJob($queueName, $class, $data, --$retryAttempts);
            }
            $this->errorLog("Exception queuing {$class} job - {$e->getMessage()}");

            throw new JobException("Exception queuing job  - {$e->getMessage()}", $e->getCode(), $e);
        }
    }

    /**
     * Actually enqueues the job with Resque. Returns a tracking token. For mocking.
     *
     * @param string $queueName
     * @param string $class
     * @param mixed  $data
     *
     * @internal param bool|\Tripod\Mongo\Jobs\false $tracking
     *
     * @return string
     */
    protected function enqueue($queueName, $class, $data)
    {
        return \Resque::enqueue($queueName, $class, $data, true);
    }

    /**
     * Given a token, return the job status. For mocking.
     *
     * @param string $token
     *
     * @return mixed
     */
    protected function getJobStatus($token)
    {
        $status = new \Resque_Job_Status($token);

        return $status->get();
    }

    /**
     * Take a Tripod Config Serializer and return a config array.
     *
     * @param array|ITripodConfigSerializer $configSerializer An object that implements ITripodConfigSerializer
     *
     * @return array
     */
    protected function serializeConfig($configSerializer)
    {
        if ($configSerializer instanceof ITripodConfigSerializer) {
            return $configSerializer->serialize();
        }
        if (is_array($configSerializer)) {
            return $configSerializer;
        }

        throw new \InvalidArgumentException(
            '$configSerializer must an ITripodConfigSerializer or array'
        );
    }

    /**
     * Deserialize a tripodConfigGenerator argument to a Tripod Config object.
     *
     * @param array $config The serialized Tripod config
     *
     * @return IConfigInstance
     */
    protected function deserializeConfig(array $config)
    {
        Config::setConfig($config);

        return Config::getInstance();
    }

    /**
     * Sets the Tripod config for the job.
     */
    protected function setTripodConfig()
    {
        if (isset($this->args[self::TRIPOD_CONFIG_GENERATOR])) {
            $config = $this->args[self::TRIPOD_CONFIG_GENERATOR];
        } else {
            $config = $this->args[self::TRIPOD_CONFIG_KEY];
        }
        $this->tripodConfig = $this->deserializeConfig($config);
    }

    /**
     * Returns the Tripod config required by the job.
     *
     * @return IConfigInstance
     */
    protected function getTripodConfig()
    {
        if (!isset($this->tripodConfig)) {
            $this->ensureConfig();
            $this->setTripodConfig();
        }

        return $this->tripodConfig;
    }

    /**
     * Sets the stats config for the job.
     */
    protected function setStatsConfig()
    {
        if (isset($this->args['statsConfig'])) {
            $this->statsConfig = $this->args['statsConfig'];
        }
    }

    /**
     * Tripod options to pass between jobs.
     *
     * @return array
     */
    protected function getTripodOptions()
    {
        $statsConfig = $this->getStatsConfig();
        $options = [];
        if (!empty($statsConfig)) {
            $options['statsConfig'] = $statsConfig;
        }

        return $options;
    }

    /**
     * Convenience method to pass config to job data.
     *
     * @return array
     */
    protected function generateConfigJobArgs()
    {
        $configInstance = $this->getConfigInstance();
        $args = [];
        if ($configInstance instanceof ITripodConfigSerializer) {
            $config = $configInstance->serialize();
        } else {
            $config = Config::getConfig();
        }
        if (isset($config['class'])) {
            $args[self::TRIPOD_CONFIG_GENERATOR] = $config;
        } else {
            $args[self::TRIPOD_CONFIG_KEY] = $config;
        }

        return $args;
    }
}
