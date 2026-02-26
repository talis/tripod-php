<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

return (new Config())
    ->setRiskyAllowed(false)
    ->setRules([
        '@auto' => true,
        '@PhpCsFixer' => true,
        'concat_space' => ['spacing' => 'one'],
        'increment_style' => false,
        'multiline_whitespace_before_semicolons' => ['strategy' => 'no_multi_line'],
        'ordered_types' => ['null_adjustment' => 'always_last'],
        'phpdoc_types_order' => ['null_adjustment' => 'always_last'],
        'yoda_style' => false,
        'php_unit_method_casing' => false,
        'php_unit_internal_class' => false,
        'php_unit_test_class_requires_covers' => false,
    ])
    ->setFinder(
        (new Finder())
            ->in(__DIR__)
            ->ignoreDotFiles(false)
    );
