<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Driver\ReadPreference;
use Tripod\Exceptions\Exception;
use Tripod\ITripodStat;

/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation.
 */
class ImpactedSubject
{
    private ?string $operation = null;

    /**
     * @var mixed[]
     */
    private array $resourceId;

    /**
     * @var mixed[]
     */
    private array $specTypes;

    /**
     * @var string
     */
    private $storeName;

    /**
     * @var string
     */
    private $podName;

    private ?ITripodStat $tripodStat;

    /**
     * @param string $operation
     * @param string $storeName
     * @param string $podName
     *
     * @throws Exception
     */
    public function __construct(array $resourceId, $operation, $storeName, $podName, array $specTypes = [], ?ITripodStat $stat = null)
    {
        if (!is_array($resourceId) || !array_key_exists(_ID_RESOURCE, $resourceId) || !array_key_exists(_ID_CONTEXT, $resourceId)) {
            throw new Exception('Parameter $resourceId needs to be of type array with ' . _ID_RESOURCE . ' and ' . _ID_CONTEXT . ' keys');
        }

        $this->resourceId = $resourceId;

        if (in_array($operation, [OP_VIEWS, OP_TABLES, OP_SEARCH])) {
            $this->operation = $operation;
        } else {
            throw new Exception('Invalid operation: ' . $operation);
        }

        $this->storeName = $storeName;
        $this->podName = $podName;
        $this->specTypes = $specTypes;

        if ($stat instanceof ITripodStat) {
            $this->tripodStat = $stat;
        }
    }

    public function getOperation(): ?string
    {
        return $this->operation;
    }

    /**
     * @return string
     */
    public function getPodName()
    {
        return $this->podName;
    }

    /**
     * @return mixed[]
     */
    public function getResourceId(): array
    {
        return $this->resourceId;
    }

    /**
     * @return mixed[]
     */
    public function getSpecTypes(): array
    {
        return $this->specTypes;
    }

    /**
     * @return string
     */
    public function getStoreName()
    {
        return $this->storeName;
    }

    /**
     * Serialises the data as an array.
     *
     * @return array<string, mixed[]|string>
     */
    public function toArray(): array
    {
        return [
            'resourceId' => $this->resourceId,
            'operation' => $this->operation,
            'specTypes' => $this->specTypes,
            'storeName' => $this->storeName,
            'podName' => $this->podName,
        ];
    }

    /**
     * Perform the update on the composite defined by the operation.
     */
    public function update(): void
    {
        $tripod = $this->getTripod();
        if (property_exists($this, 'tripodStat') && $this->tripodStat !== null) {
            $tripod->setStat($this->tripodStat);
        }

        $tripod->getComposite($this->operation)->update($this);
    }

    /**
     * For mocking.
     */
    protected function getTripod(): Driver
    {
        return new Driver($this->getPodName(), $this->getStoreName(), [
            'readPreference' => ReadPreference::RP_PRIMARY,
        ]);
    }
}
