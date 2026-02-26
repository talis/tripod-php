<?php

use Tripod\Config;
use Tripod\Mongo\Jobs\EnsureIndexes;
use Tripod\Timer;

require_once __DIR__ . '/common.inc.php';

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

if ($argc != 2 && $argc != 3 && $argc != 4) {
    echo "usage: php ensureIndexes.php tripodConfig.json [storeName] [forceReindex (default is false)] [background (default is true)]\n";

    exit;
}
array_shift($argv);

Config::setConfig(json_decode(file_get_contents($argv[0]), true));

$storeName = $argv[1] ?? null;
$forceReindex = (isset($argv[2]) && ($argv[2] == 'true')) ? true : false;
$background = (isset($argv[3]) && ($argv[3] == 'false')) ? false : true;

Config::getInstance()->setMongoCursorTimeout(-1);

$ei = new EnsureIndexes();

$t = new Timer();
$t->start();
echo "About to start scheduling indexing jobs for {$storeName}...\n";
$ei->createJob($storeName, $forceReindex, $background);
$t->stop();
echo "Finished scheduling ensure indexes jobs, took {$t->result()} seconds\n";
