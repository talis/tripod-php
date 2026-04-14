<?php

use MongoDB\Client;
use MongoDB\Database;

require_once __DIR__ . '/common.inc.php';

// Detects un-namespaced subjects or object uris in CBD collections of the target database. Optionally supply a base uri to match against that rather than all uris
if ($argc !== 4 && $argc !== 3) {
    echo 'usage: php discoverUnnamespacedUris.php connStr database [baseUri]';

    exit;
}

array_shift($argv);

/** @var Client $client */
$client = new Client(
    $argv[0],
    [],
    ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
);

/** @var Database $db */
$db = $client->selectDatabase($argv[1]);

/**
 * @param string      $uri
 * @param string|null $baseUri
 */
function isUnNamespaced($uri, $baseUri = null): bool
{
    if ($baseUri == null) {
        return strpos($uri, 'http://') === 0 || strpos($uri, 'https://') === 0;
    }

    return strpos($uri, $baseUri) === 0;
}

$results = [];
foreach ($db->listCollections() as $collectionInfo) {
    // @var \MongoDB\Collection $collection
    if (strpos($collectionInfo->getName(), 'CBD_') === 0) { // only process CBD_collections
        $collection = $db->selectCollection($collectionInfo->getName());
        echo sprintf('Checking out %s%s', $collectionInfo->getName(), PHP_EOL);
        $count = 0;
        foreach ($collection->find() as $doc) {
            if (!isset($doc['_id']) || !isset($doc['_id']['r'])) {
                echo '  Illegal doc: no _id or missing _id.r';
            } elseif (isUnNamespaced($doc['_id']['r'], $argv[2] ?? null)) {
                echo sprintf('  Un-namespaced subject: %s%s', $doc['_id']['r'], PHP_EOL);
                $count++;
            }

            foreach ($doc as $property => $value) {
                if (strpos($property, '_') === 0) { // ignore meta fields, _id, _version, _uts etc.
                    continue;
                }

                if (isset($value['l'])) {
                    // ignore, is a literal
                    continue;
                }

                if (isset($value['u'])) {
                    if (isUnNamespaced($value['u'], $argv[2] ?? null)) {
                        echo sprintf('  Un-namespaced object uri (single value): %s%s', $value['u'], PHP_EOL);
                        $count++;
                    }
                } else {
                    foreach ($value as $v) {
                        if (!isset($v['u'])) {
                            continue;
                        }

                        if (!isUnNamespaced($v['u'], $argv[2] ?? null)) {
                            continue;
                        }

                        echo sprintf('  Un-namespaced object uri (multiple value): %s%s', $v['u'], PHP_EOL);
                        $count++;
                    }
                }
            }
        }

        $results[] = sprintf('%s has %d un-namespaced uris', $collectionInfo->getName(), $count);
        echo sprintf('Done with %s%s', $collectionInfo->getName(), PHP_EOL);
    }
}

echo "\n" . implode("\n", $results) . "\n";
