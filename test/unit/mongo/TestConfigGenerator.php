<?php

use Tripod\Mongo\Config;

class TestConfigGenerator extends Config
{
    protected $fileName;

    private function __construct() {}

    /**
     * @return array<string, mixed>
     */
    public function serialize(): array
    {
        return ['class' => get_class($this), 'filename' => $this->fileName];
    }

    public static function deserialize(array $config): self
    {
        $instance = new self();
        $instance->fileName = $config['filename'];

        $cfg = json_decode(file_get_contents($config['filename']), true);
        $instance->loadConfig($cfg);

        return $instance;
    }
}
