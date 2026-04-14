<?php

use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;
use Tripod\Mongo\DriverBase;

require_once __DIR__ . '/common.inc.php';
require_once __DIR__ . '/../../src/tripod.inc.php';

// the global is necessary for Resque worker to send statements to
$logger = new Logger('TRIPOD-WORKER');
$logger->pushHandler(new StreamHandler('php://stderr', LogLevel::WARNING)); // resque too chatty on NOTICE & INFO. YMMV

// this is so tripod itself uses the same logger
DriverBase::$logger = new Logger('TRIPOD-JOB', [new StreamHandler('php://stderr', LogLevel::DEBUG)]);
