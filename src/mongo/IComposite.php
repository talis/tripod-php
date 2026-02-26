<?php

declare(strict_types=1);

namespace Tripod\Mongo\Composites;

use Tripod\Mongo\ImpactedSubject;

interface IComposite
{
    /**
     * Returns the operation this composite can satisfy.
     *
     * @return string
     */
    public function getOperationType();

    /**
     * Returns the subjects that this composite will need to regenerate given changes made to the underlying dataset.
     *
     * @param string $contextAlias
     *
     * @return mixed
     */
    public function getImpactedSubjects(array $subjectsAndPredicatesOfChange, $contextAlias);

    /**
     * Invalidate/regenerate the composite based on the impacted subject.
     */
    public function update(ImpactedSubject $subject);
}
