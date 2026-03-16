<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\BSON\ObjectId;
use MongoDB\Collection;
use MongoDB\Operation\FindOneAndUpdate;
use Tripod\Config;

class JobGroup
{
    private ObjectId $id;

    private ?Collection $collection = null;

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
    public function setJobCount($count): void
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

        return $updateResult->count;
    }

    public function getId(): ObjectId
    {
        return $this->id;
    }

    /**
     * For mocking.
     */
    protected function getMongoCollection(): Collection
    {
        if ($this->collection === null) {
            $config = Config::getInstance();

            $this->collection = $config->getCollectionForJobGroups($this->storeName);
        }

        return $this->collection;
    }
}
