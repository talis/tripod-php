<?php

use Tripod\Config;
use Tripod\Exceptions\ConfigException;

require_once __DIR__ . '/common.inc.php';
$options = getopt(
    'c:h',
    [
        'config:',
        'help',
    ]
);

function showUsage(): void
{
    $help = <<<'END'
        validateConfig.php

        Usage:

        php validateConfig.php -c/--config path/to/tripod-config.json [options]

        Options:
            -h --help               This help
            -c --config             path to Config configuration (required)
        END;
    echo $help;
}

if ($options === [] || $options === false || isset($options['h']) || isset($options['help']) || (!isset($options['c']) && !isset($options['config']))) {
    showUsage();

    exit;
}

$configLocation = $options['c'] ?? $options['config'];

require_once dirname(__FILE__, 3) . '/src/tripod.inc.php';

Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);

Config::setConfig(json_decode(file_get_contents($configLocation), true));

try {
    Config::getInstance();

    echo "\nConfig OK\n";
} catch (ConfigException $e) {
    echo "\nError: " . $e->getMessage() . "\n";
}
