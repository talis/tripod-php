<?php
include_once dirname(__FILE__) . '/common.inc.php';
$options = getopt(
    "c:s:q:hd:i:a",
    array(
        "config:",
        "storename:",
        "tripod-dir:",
        "arc-dir:",
        "spec:",
        "id:",
        "help",
        "stat-loader:",
        "queue",
        "async"
    )
);

function showUsage($scriptName)
{
    $help = <<<END
Usage:

php $scriptName -c/--config path/to/tripod-config.json -s/--storename store-name [options]

Options:
    -h --help               This help
    -c --config             path to MongoTripodConfig configuration (required)
    -s --storename          Store to create views for (required)
    -d --spec               Only create for specified search document specs
    -i --id                 Resource ID to regenerate search documents for
    -a --async              Generate table rows via queue
    -q --queue              Queue name to place jobs on (defaults to configured TRIPOD_APPLY_QUEUE value)

    --stat-loader           Path to script to initialize a Stat object.  Note, it *must* return an iTripodStat object!
    --tripod-dir            Path to tripod directory base
    --arc-dir               Path to ARC2 (required with --tripod-dir)

END;
    echo $help;
}

if(empty($options) || isset($options['h']) || isset($options['help']) ||
    (!isset($options['c']) && !isset($options['config'])) ||
    (!isset($options['s']) && !isset($options['storename']))
)
{
    showUsage($argv[0]);
    exit();
}
$configLocation = isset($options['c']) ? $options['c'] : $options['config'];
if(isset($options['tripod-dir']))
{
    if(isset($options['arc-dir']))
    {
        $tripodBasePath = $options['tripod-dir'];
        define('ARC_DIR', $options['arc-dir']);
    }
    else
    {
        showUsage($argv[0]);
        exit();
    }
}
else
{
    $tripodBasePath = dirname(dirname(dirname(__FILE__)));
}

require_once $tripodBasePath . '/vendor/autoload.php';
require_once $tripodBasePath . '/src/tripod.inc.php';


/**
 * @param string|null $id
 * @param string|null $specId
 * @param string|null $storeName
 * @param \Tripod\iTripodStat|null $stat
 * @param string|null $queue
 */
function generateSearchDocuments($id, $specId, $storeName, $stat = null, $queue = null)
{
    $spec = \Tripod\Mongo\Config::getInstance()->getSearchDocumentSpecification($storeName, $specId);
    if (array_key_exists("from",$spec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $specId";
        $tripod = new \Tripod\Mongo\Driver($spec['from'], $storeName, array('stat'=>$stat));
        $search = $tripod->getSearchIndexer();
        if ($id)
        {
            print " for $id....\n";
            $search->generateSearchDocuments($specId, $id, null, $queue);
        }
        else
        {
            print " for all tables....\n";
            $search->generateSearchDocuments($specId, null, null, $queue);
        }
    }
}

$t = new \Tripod\Timer();
$t->start();

\Tripod\Mongo\Config::setConfig(json_decode(file_get_contents($configLocation),true));

if(isset($options['s']) || isset($options['storename']))
{
    $storeName = isset($options['s']) ? $options['s'] : $options['storename'];
}
else
{
    $storeName = null;
}

if(isset($options['d']) || isset($options['spec']))
{
    $specId = isset($options['d']) ? $options['t'] : $options['spec'];
}
else
{
    $specId = null;
}

if(isset($options['i']) || isset($options['id']))
{
    $id = isset($options['i']) ? $options['i'] : $options['id'];
}
else
{
    $id = null;
}

$queue = null;
if(isset($options['a']) || isset($options['async']))
{
    if(isset($options['q']) || isset($options['queue']))
    {
        $queue = $options['queue'];
    }
    else
    {
        $queue = \Tripod\Mongo\Config::getInstance()->getApplyQueueName();
    }
}

$stat = null;

if(isset($options['stat-loader']))
{
    $stat = include_once $options['stat-loader'];
}

if ($specId)
{
    generateSearchDocuments($id, $specId, $storeName, $stat, $queue);
}
else
{
    foreach(\Tripod\Mongo\Config::getInstance()->getSearchDocumentSpecifications($storeName) as $searchSpec)
    {
        generateSearchDocuments($id, $searchSpec['_id'], $storeName, $stat, $queue);
    }
}

$t->stop();
print "Search documents created in ".$t->result()." secs\n";