<?php

declare(strict_types=1);

namespace Tripod\Exceptions;

/**
 * @codeCoverageIgnore
 */
class LabellerException extends Exception
{
    private ?string $target;

    public function __construct(?string $target)
    {
        $this->target = $target;
        parent::__construct('Could not label: ' . $target);
    }

    public function getTarget(): ?string
    {
        return $this->target;
    }
}
