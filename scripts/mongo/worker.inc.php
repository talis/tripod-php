<?php

use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;
use Tripod\Mongo\DriverBase;

require_once __DIR__ . '/common.inc.php';
require_once __DIR__ . '/../../src/tripod.inc.php';

// the global is necessary for Resque worker to send statements to
$logger = new Logger('TRIPOD-WORKER', [new StreamHandler('php://stderr', LogLevel::NOTICE)]);

// this is so tripod itself uses the same logger
DriverBase::$logger = $logger;
