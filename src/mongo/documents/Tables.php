<?php

namespace Tripod\Mongo\Documents;

class Tables extends \MongoDB\Model\BSONDocument
{
    public function __construct(array $input = [])
    {
        parent::__construct($this->toTableRow($input));
    }
    /**
     * Sets the array value to the modeled table row value
     *
     * @param array $data DB document array
     * @return void
     */
    public function bsonUnserialize(array $data)
    {
        $this->exchangeArray($this->toTableRow($data));
    }

    /**
     * Models the table row from the source data
     *
     * @param array $doc Database document
     * @return array
     */
    protected function toTableRow(array $doc)
    {
        $result = isset($doc['value']) ? $doc['value'] : [];
        if (isset($result[_IMPACT_INDEX])) {
            unset($result[_IMPACT_INDEX]);
        }
        return $result;
    }
}
