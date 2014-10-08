<?php

require_once TRIPOD_DIR . 'mongo/base/MongoTripodBase.class.php';

class MongoTripodViews extends MongoTripodBase implements SplObserver
{

    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * MongoTripod and should inherit connections set up there
     * @param MongoCollection $collection
     * @param $defaultContext
     * @param null $stat
     */
    function __construct(MongoCollection $collection,$defaultContext,$stat=null)
    {
        $this->labeller = new MongoTripodLabeller();
        $this->collection = $collection;
        $this->collectionName = $collection->getName();
        $this->defaultContext = $defaultContext;
        $this->stat = $stat;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Receive update from subject
     * @link http://php.net/manual/en/splobserver.update.php
     * @param SplSubject $subject <p>
     * The <b>SplSubject</b> notifying the observer of an update.
     * </p>
     * @return void
     */
    public function update(SplSubject $subject)
    {
        /* @var $subject ModifiedSubject */
        $queuedItem = $subject->getData();
        $resourceUri    = $queuedItem[_ID_RESOURCE];
        $context        = $queuedItem[_ID_CONTEXT];

        $this->generateViews(array($resourceUri),$context);
    }


    /**
     * Return all views, restricted by $filter conditions, for given $viewType
     * @param array $filter - an array, keyed by predicate, to filter by
     * @param $viewType
     * @return MongoGraph
     */
    public function getViews(Array $filter,$viewType)
    {
        $query = array("_id.type"=>$viewType);
        foreach ($filter as $predicate=>$object)
        {
            if (strpos($predicate,'$')===0)
            {
                $values = array();
                foreach ($object as $obj)
                {
                    foreach ($obj as $p=>$o) $values[] = array('value.'._GRAPHS.'.'.$p=>$o);
                }
                $query[$predicate] = $values;
            }
            else
            {
                $query['value.'._GRAPHS.'.'.$predicate] = $object;
            }
        }
        return $this->fetchGraph($query,MONGO_VIEW,VIEWS_COLLECTION);
    }

    /**
     * For given $resource, return the view of type $viewType
     * @param $resource
     * @param $viewType
     * @param null $context
     * @return MongoGraph
     */
    public function getViewForResource($resource,$viewType,$context=null)
    {
        if(empty($resource)){
            return new MongoGraph();
        }

        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $query = array( "_id" => array("r"=>$resourceAlias,"c"=>$contextAlias,"type"=>$viewType));
        $graph = $this->fetchGraph($query,MONGO_VIEW,VIEWS_COLLECTION);
        if ($graph->is_empty())
        {
            $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewType);
            if($viewSpec == null)
            {
                return new MongoGraph();
            }

            $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

            $doc = $this->config->getCollectionForCBD($fromCollection)->findOne(array( "_id" => array("r"=>$resourceAlias,"c"=>$contextAlias)));
            if($doc == NULL)
            {
                // if you are trying to generate a view for a document that doesnt exist in the collection
                // then we can just return an empty graph
                return new MongoGraph();
            }

            // generate view then try again
            $this->generateView($viewType,$resource,$context);
            return $this->fetchGraph($query,MONGO_VIEW,VIEWS_COLLECTION);
        }
        return $graph;
    }

    /**
     * For given $resources, return the views of type $viewType
     * @param array $resources
     * @param $viewType
     * @param null $context
     * @return MongoGraph
     */
    public function getViewForResources(Array $resources,$viewType,$context=null)
    {
        $contextAlias = $this->getContextAlias($context);

        $cursorSize = 101;
        if(count($resources) > 101) {
            $cursorSize = count($resources);
        }

        $query = array("_id" => array('$in' => $this->createTripodViewIdsFromResourceUris($resources,$context,$viewType)));
        $g = $this->fetchGraph($query,MONGO_VIEW,VIEWS_COLLECTION, null, $cursorSize);

        // account for missing subjects
        $returnedSubjects = $g->get_subjects();
        $missingSubjects = array_diff($resources,$returnedSubjects);
        if (!empty($missingSubjects))
        {
            $regrabResources = array();
            foreach($missingSubjects as $missingSubject)
            {
                $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewType);
                $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

                $missingSubjectAlias = $this->labeller->uri_to_alias($missingSubject);
                $doc = $this->config->getCollectionForCBD($fromCollection)->findOne(array( "_id" => array("r"=>$missingSubjectAlias,"c"=>$contextAlias)));
                if($doc == NULL)
                {
                    // nothing in source CBD for this subject, there can never be a view for it
                    continue;
                }

                // generate view then try again
                $this->generateView($viewType,$missingSubject,$context);
                $regrabResources[] = $missingSubject;
            }

            if(!empty($regrabResources)) {
                // only try to regrab resources if there are any to regrab
                $cursorSize = 101;
                if(count($regrabResources) > 101) {
                    $cursorSize = count($regrabResources);
                }

                $query = array("_id" => array('$in' => $this->createTripodViewIdsFromResourceUris($regrabResources,$context,$viewType)));
                $g->add_graph($this->fetchGraph($query,MONGO_VIEW,VIEWS_COLLECTION), null, $cursorSize);
            }
        }

        return $g;
    }

    private function createTripodViewIdsFromResourceUris($resourceUriOrArray,$context,$viewType)
    {
        $contextAlias = $this->getContextAlias($context);
        $ret = array();
        foreach($resourceUriOrArray as $resource)
        {
            $ret[] = array("r"=>$this->labeller->uri_to_alias($resource),"c"=>$contextAlias,"type"=>$viewType);
        }
        return $ret;
    }

    /**
     * Autodiscovers the multiple view specification that may be applicable for a given resource, and submits each for generation
     * @param $resources
     * @param null $context
     */
    public function generateViews($resources,$context=null)
    {
        $contextAlias = $this->getContextAlias($context);

        // build a filter - will be used for impactIndex detection and finding direct views to re-gen
        $filter = array();
        foreach ($resources as $resource)
        {
            $resourceAlias = $this->labeller->uri_to_alias($resource);

            // delete any views this resource is involved in. It's type may have changed so it's not enough just to regen it with it's new type below.
            foreach (MongoTripodConfig::getInstance()->getViewSpecifications() as $type=>$spec)
            {
                if($spec['from']==$this->collectionName){
                    $this->config->getCollectionForView($type)->remove(array("_id" => array("r"=>$resourceAlias,"c"=>$contextAlias,"type"=>$type)));
                }
            }

            // build $filter for queries to impact index
            $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);
        }

        // now generate view for $resources themselves... Maybe an optimisation down the line to cut out the query here
        $query = array("_id"=>array('$in'=>$filter));
        $resourceAndType = $this->collection->find($query,array("_id"=>1,"rdf:type"=>1));

        foreach ($resourceAndType as $rt)
        {
            $id = $rt["_id"];
            if (array_key_exists("rdf:type",$rt))
            {
                if (array_key_exists('u',$rt["rdf:type"]))
                {
                    // single type, not an array of values
                    $this->generateViewsForResourcesOfType($rt["rdf:type"]['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                }
                else
                {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type)
                    {
                        $this->generateViewsForResourcesOfType($type['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                    }
                }
            }
        }
    }

    /**
     * This method finds all the view specs for the given $rdfType and generates the views for the $resource one by one
     * @param $rdfType
     * @param null $resource
     * @param null $context
     * @throws Exception
     * @return mixed
     */
    public function generateViewsForResourcesOfType($rdfType,$resource=null,$context=null)
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;
        $viewSpecs = MongoTripodConfig::getInstance()->getViewSpecifications();
        foreach($viewSpecs as $key=>$viewSpec)
        {
            // check for rdfType and rdfTypeAlias
            if (
                ($viewSpec["type"]==$rdfType || (is_array($viewSpec["type"]) && in_array($rdfType,$viewSpec["type"]))) ||
                ($viewSpec["type"]==$rdfTypeAlias || (is_array($viewSpec["type"]) && in_array($rdfTypeAlias,$viewSpec["type"]))) )
            {
                $foundSpec = true;
                $this->debugLog("Processing {$viewSpec['_id']}");
                $this->generateView($key,$resource,$context);
            }
        }
        if (!$foundSpec)
        {
            $this->debugLog("Could not find any view specifications for $resource with resource type '$rdfType'");
            return;
        }
    }

    /**
     * This method will delete all views where the _id.type of the viewmatches the specified $viewId
     * @param $viewId
     * @internal param $tableId
     */
    public function deleteViewsByViewId($viewId){
        $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewId);
        if ($viewSpec==null)
        {
            $this->debugLog("Could not find a view specification with viewId '$viewId'");
            return;
        }

        $this->config->getCollectionForView($viewId)->remove(array("_id.type"=>$viewId), array('fsync'=>true));
    }

    /**
     * Given a specific $viewId, generates a single view for the $resource
     * @param $viewId
     * @param null $resource
     * @param null $context
     * @throws TripodViewException
     * @return array
     */
    public function generateView($viewId,$resource=null,$context=null)
    {
        $contextAlias = $this->getContextAlias($context);
        $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewId);
        if ($viewSpec==null)
        {
            $this->debugLog("Could not find a view specification for $resource with viewId '$viewId'");
            return null;
        }
        else
        {
            $t = new Timer();
            $t->start();

            $from = $this->getFromCollectionForViewSpec($viewSpec);

            if (!isset($viewSpec['joins']))
            {
                throw new TripodViewException('Could not find any joins in view specification - usecase better served with select()');
            }

            // ensure both the ID field and the impactIndex indexes are correctly set up
            $this->config->getCollectionForView($viewId)->ensureIndex(array('_id.r'=>1, '_id.c'=>1,'_id.type'=>1),array('background'=>1));
            $this->config->getCollectionForView($viewId)->ensureIndex(array('value.'._IMPACT_INDEX=>1),array('background'=>1));

            // ensure any custom view indexes
            if (isset($viewSpec['ensureIndexes']))
            {
                foreach ($viewSpec['ensureIndexes'] as $ensureIndex)
                {
                    $this->config->getCollectionForView($viewId)->ensureIndex($ensureIndex,array('background'=>1));
                }
            }

            $types = array(); // this is used to filter the CBD table to speed up the view creation
            if (is_array($viewSpec["type"]))
            {
                foreach ($viewSpec["type"] as $type)
                {
                    $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($type));
                    $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($type));
                }
            }
            else
            {
                $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($viewSpec["type"]));
                $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($viewSpec["type"]));
            }
            $filter = array('$or'=> $types);
            if (isset($resource))
            {
                $resourceAlias = $this->labeller->uri_to_alias($resource);
                $filter["_id"] = array(_ID_RESOURCE=>$resourceAlias,_ID_CONTEXT=>$contextAlias);
            }

            $docs = $this->config->getCollectionForCBD($from)->find($filter);
            foreach ($docs as $doc)
            {
                // set up ID
                $generatedView = array("_id"=>array(_ID_RESOURCE=>$doc["_id"][_ID_RESOURCE],_ID_CONTEXT=>$doc["_id"][_ID_CONTEXT],_ID_TYPE=>$viewSpec['_id']));
                $value = array(); // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R

                $value[_GRAPHS] = array();

                $buildImpactIndex=true;
                if (isset($viewSpec['ttl']))
                {
                    $buildImpactIndex=false;
                    $value[_EXPIRES] = new MongoDate($this->getExpirySecFromNow($viewSpec['ttl']));
                }
                else
                {
                    $value[_IMPACT_INDEX] = array($doc['_id']);
                }

                $this->doJoins($doc,$viewSpec['joins'],$value,$from,$contextAlias,$buildImpactIndex);

                // add top level properties
                $value[_GRAPHS][] = $this->extractProperties($doc,$viewSpec,$from);

                $generatedView['value'] = $value;

                $this->config->getCollectionForView($viewId)->save($generatedView);
            }

            $t->stop();
            $this->timingLog(MONGO_CREATE_VIEW, array(
                'view'=>$viewSpec['type'],
                'duration'=>$t->result(),
                'filter'=>$filter,
                'from'=>$from));
            $this->getStat()->timer(MONGO_CREATE_VIEW.".$viewId",$t->result());
        }
    }

    /**
     * Joins data to $dest from $source according to specification in $joins, or queries DB if data is not available in $source.
     * @param $source
     * @param $joins
     * @param $dest
     * @param $from
     * @param $contextAlias
     * @param bool $buildImpactIndex
     */
    protected function doJoins($source, $joins, &$dest, $from, $contextAlias, $buildImpactIndex=true)
    {
        // expand sequences before doing any joins...
        $this->expandSequence($joins,$source);

        foreach ($joins as $predicate=>$ruleset) {
            if ($predicate=='followSequence') {
                continue;
            }

            if (isset($source[$predicate]))
            {
                // todo: perhaps we can get better performance by detecting whether or not
                // the uri to join on is already in the impact index, and if so not attempting
                // to join on it. However, we need to think about different combinations of
                // nested joins in different points of the view spec and see if this would
                // complicate things. Needs a unit test or two.
                $joinUris = array();
                if (isset($source[$predicate][VALUE_URI]))
                {
                    // single value for join
                    $joinUris[] = array(_ID_RESOURCE=>$source[$predicate][VALUE_URI],_ID_CONTEXT=>$contextAlias);
                }
                else
                {
                    // multiple values for join
                    $joinsPushed = 0;
                    foreach ($source[$predicate] as $v)
                    {
                        if (isset($ruleset['maxJoins']) && !$joinsPushed<$ruleset['maxJoins'])
                        {
                            break; // maxJoins reached
                        }
                        $joinUris[] = array(_ID_RESOURCE=>$v[VALUE_URI],_ID_CONTEXT=>$contextAlias);
                        $joinsPushed++;
                    }
                }

                $recursiveJoins = array();
                $collection = isset($ruleset['from']) ? $this->config->getCollectionForCBD($ruleset['from']) : $this->config->getCollectionForCBD($from);
                $cursor = $collection->find(array('_id'=>array('$in'=>$joinUris)));
                foreach($cursor as $linkMatch) {
                    // if there is a condition, check it...
                    if (isset($ruleset['condition']))
                    {
                        $ruleset['condition']['._id'] = $linkMatch['_id'];
                    }
                    if (!(isset($ruleset['condition']) && $collection->count($ruleset['condition'])==0))
                    {
                        if ($buildImpactIndex && !isset($dest[_IMPACT_INDEX])) $dest[_IMPACT_INDEX] = array();

                        // add linkMatch if there isn't already a graph for it in the dest obj
//                        $addItemToImpactIndex = true;
                        if ($buildImpactIndex)
                        {
                            // todo: this code commented to obtain parity with M-R version, which erroneously added IDs multiple times to impact index.
                            // See if can be optimised with array_search as this is a trifle slow
//                            $addToIndex = true;
//                            foreach ($dest[_IMPACT_INDEX] as $iiItem)
//                            {
//                                if (($linkMatch['_id'][_ID_RESOURCE] === $iiItem[_ID_RESOURCE]) && ($linkMatch['_id'][_ID_CONTEXT] === $iiItem[_ID_CONTEXT]))
//                                {
//                                    $addToIndex=false;
//                                    break; // no need to continue checking
//                                }
//                            }
//                            if ($addToIndex)
                            $dest[_IMPACT_INDEX][] = $linkMatch['_id'];
                        }

                        // make sure any sequences are expanded before extracting properties
                        if (isset($ruleset['joins'])) $this->expandSequence($ruleset['joins'],$linkMatch);

                        $dest[_GRAPHS][] = $this->extractProperties($linkMatch,$ruleset,$from);

                        if (isset($ruleset['joins']))
                        {
                            // recursive joins must be done after this cursor has completed, otherwise things get messy
                            $recursiveJoins[] = array('data'=>$linkMatch, 'ruleset'=>$ruleset['joins']);
                        }
                    }
                }
                if (count($recursiveJoins)>0)
                {
                    foreach ($recursiveJoins as $r)
                    {
                        $this->doJoins($r['data'],$r['ruleset'],$dest,$from,$contextAlias,$buildImpactIndex);
                    }
                }
            }
        }
        return;
    }


    /**
     * Returns a document with properties extracted from $source, according to $viewSpec. Useful for partial representations
     * of CBDs in a view
     * @param $source
     * @param $viewSpec
     * @param $from
     * @return array
     */
    protected function extractProperties($source,$viewSpec,$from)
    {
        $obj = array();
        if (isset($viewSpec['include']))
        {
            $obj['_id'] = $source['_id'];
            foreach ($viewSpec['include'] as $p)
            {
                if(isset($source[$p]))
                {
                    $obj[$p] = $source[$p];
                }
            }
            if (isset($viewSpec['joins']))
            {
                foreach ($viewSpec['joins'] as $p=>$join)
                {
                    if (isset($join['maxJoins']))
                    {
                        // todo: refactor with below (extract method)
                        // only include up to maxJoins
                        for ($i=0;$i<$join['maxJoins'];$i++)
                        {
                            if(isset($source[$p]) && (isset($source[$p][VALUE_URI]) || isset($source[$p][VALUE_LITERAL])) && $i==0) // cater for source with only one val
                            {
                                $obj[$p] = $source[$p];
                            }
                            if(isset($source[$p]) && isset($source[$p][$i]))
                            {
                                if (!isset($obj[$p])) $obj[$p] = array();
                                $obj[$p][] = $source[$p][$i];
                            }
                        }
                    }
                    else if(isset($source[$p]))
                    {
                        $obj[$p] = $source[$p];
                    }
                }
            }
        }
        else
        {
            foreach($source as $p=>$val)
            {
                if (isset($viewSpec['joins']) && isset($viewSpec['joins'][$p]) && isset($viewSpec['joins'][$p]['maxJoins']))
                {
                    // todo: refactor with above (extract method)
                    // only include up to maxJoins
                    for ($i=0;$i<$viewSpec['joins'][$p]['maxJoins'];$i++)
                    {
                        if($val && (isset($val[VALUE_URI]) || isset($val[VALUE_LITERAL])) && $i==0) // cater for source with only one val
                        {
                            $obj[$p] = $val;
                        }
                        if($val && isset($val[$i]))
                        {
                            if (!$obj[$p]) $obj[$p] = array();
                            $obj[$p][] = $val[$i];
                        }
                    }
                }
                else
                {
                    $obj[$p] = $val;
                }
            }
        }

        // process count aggregate function
        if (isset($viewSpec['counts']))
        {
            foreach ($viewSpec['counts'] as $predicate=>$c)
            {
                if (isset($c['filter'])) // run a db filter
                {
                    $collection = isset($c['from']) ? $this->config->getCollectionForCBD($c['from']) : $this->config->getCollectionForCBD($from);
                    $query = $c['filter'];
                    $query[$c['property'].'.'.VALUE_URI] = $source['_id'][_ID_RESOURCE]; //todo: how does graph restriction work here?
                    $obj[$predicate] = array(VALUE_LITERAL=>$collection->count($query).''); // make sure it's a string
                }
                else // just look for property in current source...
                {
                    $count = 0;
                    // just count predicates at current location
                    if (isset($source[$c['property']]))
                    {
                        if (isset($source[$c['property']][VALUE_URI]) || isset($source[$c['property']][VALUE_LITERAL]))
                        {
                            $count = 1;
                        }
                        else
                        {
                            $count = count($source[$c['property']]);
                        }
                    }
                    $obj[$predicate] = array(VALUE_LITERAL=>(string)$count);
                }
            }
        }

        return $obj;
    }

    private function getFromCollectionForViewSpec($viewSpec)
    {
        $from = null;
        if (isset($viewSpec["from"]))
        {
            $from = $viewSpec["from"];
        }
        else
        {
            $from = $this->collectionName;
        }
        return $from;
    }
}