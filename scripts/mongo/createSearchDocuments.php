<?php

use Tripod\Config;
use Tripod\iTripodStat;
use Tripod\Mongo\Driver;
use Tripod\Timer;

require_once __DIR__ . '/common.inc.php';
$options = getopt(
    'c:s:q:hd:i:a',
    [
        'config:',
        'storename:',
        'spec:',
        'id:',
        'help',
        'stat-loader:',
        'queue',
        'async',
    ]
);

function showUsage($scriptName): void
{
    $help = <<<END
        Usage:

        php {$scriptName} -c/--config path/to/tripod-config.json -s/--storename store-name [options]

        Options:
            -h --help               This help
            -c --config             path to MongoTripodConfig configuration (required)
            -s --storename          Store to create views for (required)
            -d --spec               Only create for specified search document specs
            -i --id                 Resource ID to regenerate search documents for
            -a --async              Generate table rows via queue
            -q --queue              Queue name to place jobs on (defaults to configured TRIPOD_APPLY_QUEUE value)

            --stat-loader           Path to script to initialize a Stat object.  Note, it *must* return an iTripodStat object!

        END;
    echo $help;
}

if ($options === [] || $options === false || isset($options['h']) || isset($options['help'])
    || (!isset($options['c']) && !isset($options['config']))
    || (!isset($options['s']) && !isset($options['storename']))
) {
    showUsage();

    exit;
}

$configLocation = $options['c'] ?? $options['config'];

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

/**
 * @param string|null      $id
 * @param string|null      $specId
 * @param string|null      $storeName
 * @param iTripodStat|null $stat
 * @param string|null      $queue
 */
function generateSearchDocuments($id, $specId, $storeName, $stat = null, $queue = null): void
{
    $spec = Config::getInstance()->getSearchDocumentSpecification($storeName, $specId);
    if (array_key_exists('from', $spec)) {
        Config::getInstance()->setMongoCursorTimeout(-1);

        echo 'Generating ' . $specId;
        $tripod = new Driver($spec['from'], $storeName, ['stat' => $stat]);
        $search = $tripod->getSearchIndexer();
        if ($id) {
            echo " for {$id}....\n";
            $search->generateSearchDocuments($specId, $id, null, $queue);
        } else {
            echo " for all tables....\n";
            $search->generateSearchDocuments($specId, null, null, $queue);
        }
    }
}

$t = new Timer();
$t->start();

Config::setConfig(json_decode(file_get_contents($configLocation), true));

$storeName = isset($options['s']) || isset($options['storename']) ? $options['s'] ?? $options['storename'] : null;

if (isset($options['d']) || isset($options['spec'])) {
    $specId = isset($options['d']) ? $options['t'] : $options['spec'];
} else {
    $specId = null;
}

$id = isset($options['i']) || isset($options['id']) ? $options['i'] ?? $options['id'] : null;

$queue = null;
if (isset($options['a']) || isset($options['async'])) {
    if (isset($options['q']) || isset($options['queue'])) {
        $queue = $options['queue'];
    } else {
        $queue = Config::getInstance()->getApplyQueueName();
    }
}

$stat = null;

if (isset($options['stat-loader'])) {
    $stat = require_once $options['stat-loader'];
}

if ($specId) {
    generateSearchDocuments($id, $specId, $storeName, $stat, $queue);
} else {
    foreach (Config::getInstance()->getSearchDocumentSpecifications($storeName) as $searchSpec) {
        generateSearchDocuments($id, $searchSpec['_id'], $storeName, $stat, $queue);
    }
}

$t->stop();
echo 'Search documents created in ' . $t->result() . " secs\n";
