<?php

use Tripod\Config;
use Tripod\Mongo\TriplesUtil;

require_once __DIR__ . '/common.inc.php';

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

ini_set('memory_limit', '32M');

if ($argc !== 1) {
    echo "usage: php detectNamespaces.php < triples\n";

    exit;
}

array_shift($argv);

$dummyDbConfig = [
    'database' => 'foo',
    'collection' => 'bar',
    'connStr' => 'baz',
];
$config = [
    'namespaces' => [],
    'defaultContext' => 'http://example.com/',
    'transaction_log' => $dummyDbConfig,
    'es_config' => [
        'endpoint' => 'http://example.com/',
        'indexes' => [],
        'search_document_specifications' => [],
    ],
    'queue' => $dummyDbConfig,
    'databases' => [
        'default' => [
            'connStr' => 'baz',
            'collections' => [],
        ],
    ],
];

Config::setConfig($config);

$util = new TriplesUtil();
$objectNs = [];
$i = 0;
while (($line = fgets(STDIN)) !== false) {
    $i++;

    $line = rtrim($line);
    $parts = preg_split('/\s/', $line);
    $subject = trim($parts[0], '><');

    if ($i % 2500 === 0) {
        echo '.';
    }

    if ($i % 50000 === 0) {
        foreach ($objectNs as $key => $val) {
            if ($val < 5) {
                // flush
                unset($objectNs[$key]);
            }
        }

        gc_collect_cycles();
        echo 'F';
    }

    if (empty($currentSubject)) { // set for first iteration
        $currentSubject = $subject;
    } elseif ($currentSubject != $subject) { // once subject changes, we have all triples for that subject, flush to Mongo
        $ns = $util->extractMissingPredicateNs($triples);
        if (count($ns) > 0) {
            $newNsConfig = [];
            foreach ($ns as $n) {
                $prefix = $util->suggestPrefix($n);
                if (array_key_exists($prefix, $config['namespaces'])) {
                    $prefix .= uniqid();
                }

                $newNsConfig[$prefix] = $n;
                echo "\nFound ns {$n} suggest prefix {$prefix}";
                $config['namespaces'] = array_merge($config['namespaces'], $newNsConfig);
                Config::setConfig($config);
            }
        }

        $ns = $util->extractMissingObjectNs($triples);
        if (count($ns) > 0) {
            $newNsConfig = [];
            foreach ($ns as $n) {
                if (array_key_exists($n, $objectNs)) {
                    $objectNs[$n]++;
                } else {
                    $objectNs[$n] = 1;
                }

                if ($objectNs[$n] > 500) {
                    $prefix = $util->suggestPrefix($n);
                    if (array_key_exists($prefix, $config['namespaces'])) {
                        $prefix .= uniqid();
                    }

                    $newNsConfig[$prefix] = $n;
                    echo "\nFound object ns {$n} occurs > 500 times, suggest prefix {$prefix}";
                    $config['namespaces'] = array_merge($config['namespaces'], $newNsConfig);
                    Config::setConfig($config);
                }
            }
        }

        $currentSubject = $subject; // reset current subject to next subject
        $triples = []; // reset triples
    }

    $triples[] = $line;
}

echo "Suggested namespace configuration:\n\n";

/**
 * @param string $json
 */
function indent($json): string
{
    $result = '';
    $pos = 0;
    $strLen = strlen($json);
    $indentStr = '  ';
    $newLine = "\n";
    $prevChar = '';
    $outOfQuotes = true;

    for ($i = 0; $i <= $strLen; $i++) {
        // Grab the next character in the string.
        $char = substr($json, $i, 1);

        // Are we inside a quoted string?
        if ($char === '"' && $prevChar !== '\\') {
            $outOfQuotes = !$outOfQuotes;

        // If this character is the end of an element,
        // output a new line and indent the next line.
        } elseif (($char === '}' || $char === ']') && $outOfQuotes) {
            $result .= $newLine;
            $pos--;
            for ($j = 0; $j < $pos; $j++) {
                $result .= $indentStr;
            }
        }

        // Add the character to the result string.
        $result .= $char;

        // If the last character was the beginning of an element,
        // output a new line and indent the next line.
        if ((in_array($char, [',', '{', '['])) && $outOfQuotes) {
            $result .= $newLine;
            if ($char === '{' || $char === '[') {
                $pos++;
            }

            for ($j = 0; $j < $pos; $j++) {
                $result .= $indentStr;
            }
        }

        $prevChar = $char;
    }

    return $result;
}

$json = json_encode(['namespaces' => $config['namespaces']]);

echo indent($json);
