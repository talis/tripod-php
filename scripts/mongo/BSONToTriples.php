<?php

use Tripod\Config;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TriplesUtil;

require_once dirname(__FILE__) . '/common.inc.php';

require_once dirname(dirname(dirname(__FILE__))) . '/src/tripod.inc.php';

if ($argc != 2) {
    echo "usage: ./BSONToTriples.php tripodConfig.json < bsondata\n";
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

    $graph = new MongoGraph();
    $doc = json_decode($line, true);

    if (array_key_exists('_id', $doc)) {
        $subject = $doc['_id'];

        unset($doc['_id']);
        if (array_key_exists('_version', $doc)) {
            unset($doc['_version']);
        }

        foreach ($doc as $property => $values) {
            if (isset($values['value'])) {
                $doc[$property] = [$values];
            }
        }

        foreach ($doc as $property => $values) {
            foreach ($values as $value) {
                if ($value['type'] == 'literal') {
                    $graph->add_literal_triple($subject, $graph->qname_to_uri($property), $value['value']);
                } else {
                    $graph->add_resource_triple($subject, $graph->qname_to_uri($property), $value['value']);
                }
            }
        }

        echo $graph->to_ntriples();
    }
}
