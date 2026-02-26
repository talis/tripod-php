<?php

declare(strict_types=1);

namespace Tripod\Mongo\Jobs;

use Tripod\Mongo\Composites\IComposite;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Timer;

class DiscoverImpactedSubjects extends JobBase
{
    public const STORE_NAME_KEY = 'storeName';

    public const POD_NAME_KEY = 'podName';

    public const OPERATIONS_KEY = 'operations';

    public const CHANGES_KEY = 'changes';

    public const CONTEXT_ALIAS_KEY = 'contextAlias';

    /**
     * @var ApplyOperation
     */
    protected $applyOperation;

    /**
     * @var array
     */
    protected $subjectsGroupedByQueue = [];

    protected $configRequired = true;

    protected $subjectCount;

    protected $mandatoryArgs = [
        self::STORE_NAME_KEY,
        self::POD_NAME_KEY,
        self::CHANGES_KEY,
        self::OPERATIONS_KEY,
        self::CONTEXT_ALIAS_KEY,
    ];

    public function tearDown(): void
    {
        parent::tearDown();
        $this->getStat()->increment(MONGO_QUEUE_DISCOVER_JOB . '.' . SUBJECT_COUNT, $this->subjectCount);
    }

    /**
     * Run the DiscoverImpactedSubjects job.
     *
     * @throws \Exception
     */
    public function perform(): void
    {
        $tripod = $this->getTripod(
            $this->args[self::STORE_NAME_KEY],
            $this->args[self::POD_NAME_KEY],
            $this->getTripodOptions()
        );

        $operations = $this->args[self::OPERATIONS_KEY];

        $subjectsAndPredicatesOfChange = $this->args[self::CHANGES_KEY];

        $this->subjectCount = 0;
        foreach ($operations as $op) {
            /** @var IComposite $composite */
            $composite = $tripod->getComposite($op);
            $modifiedSubjects = $composite->getImpactedSubjects(
                $subjectsAndPredicatesOfChange,
                $this->args[self::CONTEXT_ALIAS_KEY]
            );
            if (!empty($modifiedSubjects)) {
                $configInstance = $this->getConfigInstance();
                // @var $subject \Tripod\Mongo\ImpactedSubject
                foreach ($modifiedSubjects as $subject) {
                    $this->subjectCount++;
                    $subjectTimer = new Timer();
                    $subjectTimer->start();
                    if (isset($this->args[self::QUEUE_KEY]) || count($subject->getSpecTypes()) === 0) {
                        $queueName = $this->args[self::QUEUE_KEY] ?? $configInstance::getApplyQueueName();

                        $this->addSubjectToQueue($subject, $queueName);
                    } else {
                        $specsGroupedByQueue = [];
                        foreach ($subject->getSpecTypes() as $specType) {
                            $spec = null;

                            switch ($subject->getOperation()) {
                                case OP_VIEWS:
                                    $spec = $configInstance->getViewSpecification(
                                        $this->args[self::STORE_NAME_KEY],
                                        $specType
                                    );

                                    break;

                                case OP_TABLES:
                                    $spec = $configInstance->getTableSpecification(
                                        $this->args[self::STORE_NAME_KEY],
                                        $specType
                                    );

                                    break;

                                case OP_SEARCH:
                                    $spec = $configInstance->getSearchDocumentSpecification(
                                        $this->args[self::STORE_NAME_KEY],
                                        $specType
                                    );

                                    break;
                            }

                            if (!$spec || !isset($spec['queue'])) {
                                if (!$spec) {
                                    $spec = [];
                                }

                                $spec['queue'] = $configInstance::getApplyQueueName();
                            }

                            if (!isset($specsGroupedByQueue[$spec['queue']])) {
                                $specsGroupedByQueue[$spec['queue']] = [];
                            }

                            $specsGroupedByQueue[$spec['queue']][] = $specType;
                        }

                        foreach ($specsGroupedByQueue as $queueName => $specs) {
                            $queuedSubject = new ImpactedSubject(
                                $subject->getResourceId(),
                                $subject->getOperation(),
                                $subject->getStoreName(),
                                $subject->getPodName(),
                                $specs
                            );

                            $this->addSubjectToQueue($queuedSubject, $queueName);
                        }
                    }

                    $subjectTimer->stop();
                    // stat time taken to discover impacted subjects for the given subject of change
                    $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUBJECT, $subjectTimer->result());
                }

                if ($this->subjectsGroupedByQueue !== []) {
                    foreach ($this->subjectsGroupedByQueue as $queueName => $subjects) {
                        $this->getApplyOperation()->createJob($subjects, $queueName, $this->getTripodOptions());
                    }

                    $this->subjectsGroupedByQueue = [];
                }
            }
        }
    }

    /**
     * @param string|null $queueName
     */
    public function createJob(array $data, $queueName = null): void
    {
        $configInstance = $this->getConfigInstance();
        if (!$queueName) {
            $queueName = $configInstance::getDiscoverQueueName();
        } elseif (strpos($queueName, $configInstance::getDiscoverQueueName()) === false) {
            $queueName = $configInstance::getDiscoverQueueName() . '::' . $queueName;
        }

        $this->submitJob($queueName, get_class($this), array_merge($data, $this->generateConfigJobArgs()));
    }

    /**
     * Stat string for successful job timer.
     */
    protected function getStatTimerSuccessKey(): string
    {
        return MONGO_QUEUE_DISCOVER_SUCCESS;
    }

    /**
     * Stat string for failed job increment.
     */
    protected function getStatFailureIncrementKey(): string
    {
        return MONGO_QUEUE_DISCOVER_FAIL;
    }

    /**
     * @param string $queueName
     */
    protected function addSubjectToQueue(ImpactedSubject $subject, $queueName)
    {
        if (!array_key_exists($queueName, $this->subjectsGroupedByQueue)) {
            $this->subjectsGroupedByQueue[$queueName] = [];
        }

        $this->subjectsGroupedByQueue[$queueName][] = $subject;
    }

    /**
     * For mocking.
     *
     * @return ApplyOperation
     */
    protected function getApplyOperation()
    {
        if ($this->applyOperation === null) {
            $this->applyOperation = new ApplyOperation();
        }

        return $this->applyOperation;
    }
}
