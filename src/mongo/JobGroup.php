<?php

namespace Tripod\Mongo;

use MongoDB\BSON\ObjectId;
use MongoDB\Collection;
use MongoDB\Operation\FindOneAndUpdate;
use Tripod\Config;

class JobGroup
{
    private $id;
    private $collection;
    private $storeName;

    /**
     * Constructor method.
     *
     * @param string          $storeName Tripod store (database) name
     * @param ObjectId|string $groupId   Optional tracking ID, will assign a new one if omitted
     */
    public function __construct($storeName, $groupId = null)
    {
        $this->storeName = $storeName;
        if (!$groupId) {
            $groupId = new ObjectId();
        } elseif (!$groupId instanceof ObjectId) {
            $groupId = new ObjectId($groupId);
        }
        $this->id = $groupId;
    }

    /**
     * Update the number of jobs.
     *
     * @param int $count Number of jobs in group
     */
    public function setJobCount($count)
    {
        $this->getMongoCollection()->updateOne(
            ['_id' => $this->getId()],
            ['$set' => ['count' => $count]],
            ['upsert' => true]
        );
    }

    /**
     * Update the number of jobs by $inc.  To decrement, use a negative integer.
     *
     * @param int $inc Number to increment or decrement by
     *
     * @return int Updated job count
     */
    public function incrementJobCount($inc = 1)
    {
        $updateResult = $this->getMongoCollection()->findOneAndUpdate(
            ['_id' => $this->getId()],
            ['$inc' => ['count' => $inc]],
            ['upsert' => true, 'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
        );
        if (\is_array($updateResult)) {
            return $updateResult['count'];
        }
        if (isset($updateResult->count)) {
            return $updateResult->count;
        }
    }

    /**
     * @return ObjectId
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * For mocking.
     *
     * @return Collection
     */
    protected function getMongoCollection()
    {
        if (!isset($this->collection)) {
            $config = Config::getInstance();

            $this->collection = $config->getCollectionForJobGroups($this->storeName);
        }

        return $this->collection;
    }
}
