<?php

use Tripod\Config;
use Tripod\Mongo\TriplesUtil;

require_once __DIR__ . '/common.inc.php';

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

if ($argc != 2) {
    echo "usage: ./triplesToBSON.php tripodconfig.json < ntriplesdata\n";

    exit;
}
array_shift($argv);

$config = json_decode(file_get_contents($argv[0]), true);
Config::setConfig($config);

$currentSubject = '';
$triples = [];
$docs = [];
$errors = []; // array of subjects that failed to insert, even after retry...
$tu = new TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);

    $parts = preg_split('/\s/', $line);
    $subject = trim($parts[0], '><');

    if (empty($currentSubject)) { // set for first iteration
        $currentSubject = $subject;
    }

    if ($currentSubject != $subject) { // once subject changes, we have all triples for that subject, flush to Mongo
        echo json_encode($tu->getTArrayAbout($currentSubject, $triples, Config::getInstance()->getDefaultContextAlias())) . "\n";
        $currentSubject = $subject; // reset current subject to next subject
        $triples = []; // reset triples
    }

    $triples[] = $line;
}

// last doc
echo json_encode($tu->getTArrayAbout($currentSubject, $triples, Config::getInstance()->getDefaultContextAlias())) . "\n";

?>

