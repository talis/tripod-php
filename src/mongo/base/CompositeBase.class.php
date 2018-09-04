<?php

namespace Tripod\Mongo\Composites;

use \Tripod\Mongo\JobGroup;

/**
 * Class CompositeBase
 * @package Tripod\Mongo\Composites
 */
abstract class CompositeBase extends \Tripod\Mongo\DriverBase implements \Tripod\Mongo\Composites\IComposite
{
    /**
     * @var \Tripod\Mongo\Jobs\ApplyOperation
     */
    protected $applyOperation;

    /**
     * Returns an array of ImpactedSubjects based on the subjects and predicates of change
     * @param array $subjectsAndPredicatesOfChange
     * @param string $contextAlias
     * @return \Tripod\Mongo\ImpactedSubject[]
     */
    public function getImpactedSubjects(array $subjectsAndPredicatesOfChange, $contextAlias)
    {
        $candidates = [];
        $filter = [];
        $subjectsToAlias = [];
        foreach (array_keys($subjectsAndPredicatesOfChange) as $s) {
            $resourceAlias = $this->labeller->uri_to_alias($s);
            $subjectsToAlias[$s] = $resourceAlias;
            // build $filter for queries to impact index
            $filter[] = [_ID_RESOURCE=>$resourceAlias, _ID_CONTEXT=>$contextAlias];
        }
        $query = [_ID_KEY => ['$in' => $filter]];
        $docs = $this->getCollection()->find(
            $query,
            ['projection' => [_ID_KEY => true, 'rdf:type' => true]]
        );

        $types = $this->getTypesInSpecifications();

        if ($this->getCollection()->count($query) !== 0) {
            foreach ($docs as $doc) {
                $docResource = $doc[_ID_KEY][_ID_RESOURCE];
                $docContext  = $doc[_ID_KEY][_ID_CONTEXT];
                $docHash     = md5($docResource.$docContext);

                $docTypes = [];
                if (isset($doc['rdf:type'])) {
                    if (isset($doc['rdf:type'][VALUE_URI])) {
                        $docTypes[] = $doc['rdf:type'][VALUE_URI];
                    } else {
                        foreach ($doc['rdf:type'] as $t) {
                            if (isset($t[VALUE_URI])) {
                                $docTypes[] = $t[VALUE_URI];
                            }
                        }
                    }
                }

                $currentSubjectProperties = [];
                if (isset($subjectsAndPredicatesOfChange[$docResource])) {
                    $currentSubjectProperties = $subjectsAndPredicatesOfChange[$docResource];
                } elseif (isset($subjectsToAlias[$docResource]) &&
                    isset($subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]])) {
                    $currentSubjectProperties = $subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]];
                }
                foreach ($docTypes as $type) {
                    if ($this->checkIfTypeShouldTriggerOperation($type, $types, $currentSubjectProperties)) {
                        if (!array_key_exists($this->getPodName(), $candidates)) {
                            $candidates[$this->getPodName()] = [];
                        }
                        if (!array_key_exists($docHash, $candidates[$this->getPodName()])) {
                            $candidates[$this->getPodName()][$docHash] = ['id'=>$doc[_ID_KEY]];
                        }
                    }
                }
            }
        }

        // add to this any composites
        foreach ($this->findImpactedComposites($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
            $spec = $this->getSpecification($this->storeName, $doc[_ID_KEY]['type']);
            if (is_array($spec) && array_key_exists('from', $spec)) {
                if (!array_key_exists($spec['from'], $candidates)) {
                    $candidates[$spec['from']] = [];
                }
                $docHash = md5($doc[_ID_KEY][_ID_RESOURCE] . $doc[_ID_KEY][_ID_CONTEXT]);

                if (!array_key_exists($docHash, $candidates[$spec['from']])) {
                    $candidates[$spec['from']][$docHash] = [
                        'id' => [
                            _ID_RESOURCE=>$doc[_ID_KEY][_ID_RESOURCE],
                            _ID_CONTEXT=>$doc[_ID_KEY][_ID_CONTEXT],
                        ]
                    ];
                }
                if (!array_key_exists('specTypes', $candidates[$spec['from']][$docHash])) {
                    $candidates[$spec['from']][$docHash]['specTypes'] = [];
                }
                // Save the specification type so we only have to regen resources in that table type
                if (!in_array($doc[_ID_KEY][_ID_TYPE], $candidates[$spec['from']][$docHash]['specTypes'])) {
                    $candidates[$spec['from']][$docHash]['specTypes'][] = $doc[_ID_KEY][_ID_TYPE];
                }
            }
        }

        // convert operations to subjects
        $impactedSubjects = [];
        foreach (array_keys($candidates) as $podName) {
            foreach ($candidates[$podName] as $candidate) {
                $specTypes = (isset($candidate['specTypes']) ? $candidate['specTypes'] : []);
                $impactedSubjects[] = new \Tripod\Mongo\ImpactedSubject($candidate['id'], $this->getOperationType(), $this->getStoreName(), $podName, $specTypes);
            }
        }

        return $impactedSubjects;
    }

    /**
     * Returns an array of the rdf types that will trigger the specification
     * @return array
     */
    abstract public function getTypesInSpecifications();

    /**
     * @param array $resourcesAndPredicates
     * @param string $contextAlias
     * @return mixed // @todo: This may eventually return a either a Cursor or array
     */
    abstract public function findImpactedComposites(array $resourcesAndPredicates, $contextAlias);

    /**
     * Returns the specification config
     * @param string $storeName
     * @param string $specId The specification id
     * @return array|null
     */
    abstract public function getSpecification($storeName, $specId);

    /**
     * Test if the a particular type appears in the array of types associated with a particular spec and that the changeset
     * includes rdf:type (or is empty, meaning addition or deletion vs. update)
     * @param string $rdfType
     * @param array $validTypes
     * @param array $subjectPredicates
     * @return bool
     */
    protected function checkIfTypeShouldTriggerOperation($rdfType, array $validTypes, array $subjectPredicates)
    {
        // We don't know if this is an alias or a fqURI, nor what is in the valid types, necessarily
        $types = [$rdfType];
        try {
            $types[] = $this->labeller->qname_to_uri($rdfType);
        } catch (\Tripod\Exceptions\LabellerException $e) {
            // Not a qname, apparently
        }
        try {
            $types[] = $this->labeller->uri_to_alias($rdfType);
        } catch (\Tripod\Exceptions\LabellerException $e) {
            // Not a declared uri, apparently
        }

        $intersectingTypes = array_unique(array_intersect($types, $validTypes));
        // If views have a matching type *at all*, the operation is triggered
        return (!empty($intersectingTypes));
    }

    /**
     * For mocking
     *
     * @return \Tripod\Mongo\Jobs\ApplyOperation
     */
    protected function getApplyOperation()
    {
        if (!isset($this->applyOperation)) {
            $this->applyOperation = new \Tripod\Mongo\Jobs\ApplyOperation();
        }
        return $this->applyOperation;
    }

    /**
     * Queues a batch of ImpactedSubjects in a single ApplyOperation job
     *
     * @param \Tripod\Mongo\ImpactedSubject[] $subjects   Array of ImpactedSubjects
     * @param string                          $queueName  Queue name
     * @param array                           $jobOptions Job options
     * @return void
     */
    protected function queueApplyJob(array $subjects, $queueName, array $jobOptions)
    {
        $this->getApplyOperation()->createJob($subjects, $queueName, $jobOptions);
    }

    /**
     * For mocking
     *
     * @param string $storeName
     * @return JobGroup
     */
    protected function getJobGroup($storeName)
    {
        return new JobGroup($storeName);
    }
}
