<?php

use Tripod\Config;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TriplesUtil;

require_once __DIR__ . '/common.inc.php';

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

if ($argc !== 2) {
    echo "usage: ./BSONToQuads.php tripodConfig.json < bsondata\n";
    echo "  When exporting bson data from Mongo use:  \n";
    echo "     mongoexport -d <dbname> -c <collectionName> > bsondata.txt \n";

    exit;
}

array_shift($argv);
$config = json_decode(file_get_contents($argv[0]), true);
Config::setConfig($config);

$tu = new TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);
    $doc = json_decode($line, true);
    $context = $doc['_id']['c'];

    $graph = new MongoGraph();
    $graph->add_tripod_array($doc);

    echo $graph->to_nquads($context);
}
