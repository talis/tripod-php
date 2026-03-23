<?php

use Psr\Log\NullLogger;
use Tripod\Mongo\DriverBase;
use Tripod\Mongo\Jobs\JobBase;

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../src/tripod.inc.php';
require_once __DIR__ . '/stubs.php';

// Mongo Config For Main DB
define('MONGO_MAIN_DB', 'acorn');
define('MONGO_MAIN_COLLECTION', 'CBD_harvest');
define('MONGO_USER_COLLECTION', 'CBD_user');

// Queue worker must register these event listeners
Resque_Event::listen('beforePerform', [JobBase::class, 'beforePerform']);
Resque_Event::listen('onFailure', [JobBase::class, 'onFailure']);

// Make sure log statements don't go to stdout during tests...
$logger = new NullLogger();
DriverBase::$logger = $logger;
