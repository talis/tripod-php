<?php

namespace Tripod\Test\Mongo\Mocks;

use MongoDB\BSON\Int64;
use MongoDB\Driver\CursorInterface;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Server;

class CursorPhp80 extends \ArrayIterator implements CursorInterface
{
    private array $array;

    public function __construct(array $array)
    {
        parent::__construct($array);
        $this->array = $array;
    }

    #[\ReturnTypeWillChange] // in ext-mongodb <1.20.0 returns MongoDB\Driver\CursorId
    public function getId(): Int64
    {
        return new Int64(0);
    }

    public function getServer(): Server
    {
        return (new Manager())->getServers()[0];
    }

    public function isDead(): bool
    {
        return false;
    }

    public function setTypeMap(array $typemap): void
    {
        // noop
    }

    public function toArray(): array
    {
        return $this->array;
    }

    public function key(): ?int
    {
        return parent::key();
    }

    public function current(): array|object|null
    {
        return parent::current();
    }
}
