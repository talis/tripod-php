<?php

namespace Tripod\Mongo\Jobs;

use \Tripod\Mongo\Config;
/**
 * Class DiscoverImpactedSubjects
 * @package Tripod\Mongo\Jobs
 */
class DiscoverImpactedSubjects extends JobBase {

    const STORE_NAME_KEY = 'storeName';
    const POD_NAME_KEY = 'podName';
    const OPERATIONS_KEY = 'operations';
    const CHANGES_KEY = 'changes';
    const CONTEXT_ALIAS_KEY = 'contextAlias';

    /**
     * @var ApplyOperation
     */
    protected $applyOperation;

    /**
     * Run the DiscoverImpactedSubjects job
     * @throws \Exception
     */
    public function perform()
    {
        try
        {

            $this->debugLog("DiscoverImpactedSubjects::perform() start");

            $timer = new \Tripod\Timer();
            $timer->start();

            $this->validateArgs();

            // set the config to what is received
            \Tripod\Mongo\Config::setConfig($this->args[self::TRIPOD_CONFIG_KEY]);

            $tripod = $this->getTripod($this->args[self::STORE_NAME_KEY],$this->args[self::POD_NAME_KEY]);

            $operations = $this->args[self::OPERATIONS_KEY];
            $modifiedSubjects = array();

            $subjectsAndPredicatesOfChange = $this->args[self::CHANGES_KEY];

            foreach($operations as $op)
            {
                /** @var \Tripod\Mongo\Composites\IComposite $composite */
                $composite = $tripod->getComposite($op);
                $modifiedSubjects = array_merge($modifiedSubjects,$composite->getImpactedSubjects($subjectsAndPredicatesOfChange,$this->args['contextAlias']));
            }

            if(!empty($modifiedSubjects)){
                /* @var $subject \Tripod\Mongo\ImpactedSubject */
                foreach ($modifiedSubjects as $subject) {
                    $subject->update();
                }
            }

            // stat time taken to process item, from time it was created (queued)
            $timer->stop();
            $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUCCESS,$timer->result());
            $this->debugLog("DiscoverImpactedSubjects::perform() done in {$timer->result()}ms");

        }
        catch(\Exception $e)
        {
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * @param array $data
     * @param string|null $queueName
     */
    public function createJob(Array $data, $queueName=null)
    {
        if(!$queueName)
        {
            $queueName = Config::getDiscoverQueueName();
        }
        $this->submitJob($queueName,get_class($this),$data);
    }

    /**
     * Validate args for DiscoverImpactedSubjects
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array(
            self::TRIPOD_CONFIG_KEY,
            self::STORE_NAME_KEY,
            self::POD_NAME_KEY,
            self::CHANGES_KEY,
            self::OPERATIONS_KEY,
            self::CONTEXT_ALIAS_KEY
        );
    }

    /**
     * @param \Tripod\Mongo\ImpactedSubject $subject
     * @param string $queueName
     */
    protected function addSubjectToQueue(\Tripod\Mongo\ImpactedSubject $subject, $queueName)
    {
        if(!array_key_exists($queueName, $this->subjectsGroupedByQueue))
        {
            $this->subjectsGroupedByQueue[$queueName] = array();
        }
        $this->subjectsGroupedByQueue[$queueName][] = $subject;
    }

    /**
     * For mocking
     * @return ApplyOperation
     */
    protected function getApplyOperation()
    {
        if(!isset($this->applyOperation))
        {
            $this->applyOperation = new ApplyOperation();
        }
        return $this->applyOperation;
    }


}
