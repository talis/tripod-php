<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\BSON\UTCDateTime;

class DateUtil
{
    /**
     * Return a UTCDateTime object
     * If you pass in your own time, it will use that to construct the object, otherwise
     * it will create an object based on the current time.
     *
     * @param float|int|null $milliseconds - time in milliseconds since the epoch
     */
    public static function getMongoDate($milliseconds = null): UTCDateTime
    {
        if (is_null($milliseconds)) {
            $milliseconds = floor(microtime(true) * 1000);
        }

        return new UTCDateTime($milliseconds);
    }
}
