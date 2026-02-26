<?php

use Tripod\Config;
use Tripod\Mongo\TriplesUtil;
use Tripod\Timer;

require_once __DIR__ . '/common.inc.php';

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

/**
 * @param string $podName
 * @param string $storeName
 */
function load(TriplesUtil $loader, string $subject, array $triples, array &$errors, $podName, $storeName): void
{
    try {
        $loader->loadTriplesAbout($subject, $triples, $storeName, $podName);
    } catch (Exception $e) {
        echo sprintf('Exception for subject %s failed with message: ', $subject) . $e->getMessage() . "\n";
        $errors[] = $subject;
    }
}

$timer = new Timer();
$timer->start();

if ($argc !== 4) {
    echo "usage: ./loadTriples.php storename podname tripodConfig.json < ntriplesdata\n";

    exit;
}

array_shift($argv);

$storeName = $argv[0];
$podName = $argv[1];
Config::setConfig(json_decode(file_get_contents($argv[2]), true));

$i = 0;
$currentSubject = '';
$triples = [];
$errors = []; // array of subjects that failed to insert, even after retry...
$loader = new TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $i++;

    if ($i % 250000 === 0) {
        echo 'Memory: ' . memory_get_usage() . "\n";
    }

    $line = rtrim($line);
    $parts = preg_split('/\s/', $line);
    $subject = trim($parts[0], '><');

    if ($currentSubject === '' || $currentSubject === '0') { // set for first iteration
        $currentSubject = $subject;
    } elseif ($currentSubject !== $subject) { // once subject changes, we have all triples for that subject, flush to Mongo
        load($loader, $currentSubject, $triples, $errors, $podName, $storeName);
        $currentSubject = $subject; // reset current subject to next subject
        $triples = []; // reset triples
    }

    $triples[] = $line;
}

// last doc
load($loader, $currentSubject, $triples, $errors, $podName, $storeName);

$timer->stop();
echo 'This script ran in ' . $timer->result() . " milliseconds\n";

echo 'Processed ' . $i . ' triples';
if ($errors !== []) {
    echo 'Insert errors on ' . count($errors) . " subjects\n";
    var_dump($errors); // todo: decide what to do with errors...
}
