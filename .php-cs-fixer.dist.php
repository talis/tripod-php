<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

return (new Config())
    ->setRiskyAllowed(false)
    ->setRules([
        '@auto' => true,
        '@PhpCsFixer' => true,
        'blank_line_before_statement' => ['statements' => [
            'break',
            'case',
            'continue',
            'declare',
            'default',
            'exit',
            'goto',
            'phpdoc',
            'return',
            'switch',
            'throw',
            'try',
        ]],
        'concat_space' => ['spacing' => 'one'],
        'increment_style' => false,
        'multiline_whitespace_before_semicolons' => ['strategy' => 'no_multi_line'],
        'ordered_types' => ['null_adjustment' => 'always_last'],
        'phpdoc_no_empty_return' => false,
        'phpdoc_types_order' => ['null_adjustment' => 'always_last'],
        'yoda_style' => false,
        'php_unit_method_casing' => false,
        'php_unit_internal_class' => false,
        'php_unit_test_class_requires_covers' => false,
    ])
    ->setFinder(
        (new Finder())
            ->in(__DIR__)
            ->ignoreVCSIgnored(true)
            ->ignoreDotFiles(false)
            ->notPath('rector.php')
            ->notPath('test/unit/mongo/mocks')
    );
