<?php

use Tripod\Config;
use Tripod\ITripodStat;
use Tripod\Mongo\Driver;
use Tripod\Timer;

require_once __DIR__ . '/common.inc.php';
$options = getopt(
    'c:s:q:hv:i:a',
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

function showUsage(): void
{
    $help = <<<'END'
        createViews.php

        Usage:

        php createViews.php -c/--config path/to/tripod-config.json -s/--storename store-name [options]

        Options:
            -h --help               This help
            -c --config             path to Config configuration (required)
            -s --storename          Store to create views for (required)
            -v --spec               Only create for specified view specs
            -i --id                 Resource ID to regenerate views for
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
 * @param string|null      $viewId
 * @param string           $storeName
 * @param ITripodStat|null $stat
 * @param string           $queue
 */
function generateViews($id, $viewId, $storeName, $stat, $queue): void
{
    $viewSpec = Config::getInstance()->getViewSpecification($storeName, $viewId);
    if (array_key_exists('from', $viewSpec)) {
        Config::getInstance()->setMongoCursorTimeout(-1);

        echo 'Generating ' . $viewId;
        $tripod = new Driver($viewSpec['from'], $storeName, ['stat' => $stat]);
        $views = $tripod->getTripodViews();
        if ($id) {
            echo " for {$id}....\n";
            $views->generateView($viewId, $id, null, $queue);
        } else {
            echo " for all views....\n";
            $views->generateView($viewId, null, null, $queue);
        }
    }
}

$t = new Timer();
$t->start();

Config::setConfig(json_decode(file_get_contents($configLocation), true));

$storeName = isset($options['s']) || isset($options['storename']) ? $options['s'] ?? $options['storename'] : null;

$viewId = isset($options['v']) || isset($options['spec']) ? $options['v'] ?? $options['spec'] : null;

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

if ($viewId) {
    generateViews($id, $viewId, $storeName, $stat, $queue);
} else {
    foreach (Config::getInstance()->getViewSpecifications($storeName) as $viewSpec) {
        generateViews($id, $viewSpec['_id'], $storeName, $stat, $queue);
    }
}

$t->stop();
echo 'Views created in ' . $t->result() . " secs\n";
