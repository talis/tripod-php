<?php

namespace Tripod\Mongo;

/**
 * Class IndexUtils
 * @package Tripod\Mongo
 */
class IndexUtils
{
    /**
     * Ensures the index for the given $storeName.
     * @param bool $reindex - force a reindex of existing data
     * @param null $storeName - database name to ensure indexes for
     * @param bool $background - index in the background (default) or lock DB whilst indexing
     */
    public function ensureIndexes($reindex=false,$storeName=null,$background=true)
    {
        error_log('ensureIndexes: '.$storeName);
        $config = $this->getConfig();
        $dbs = ($storeName==null) ? $config->getDbs() : array($storeName);
        $reindexedCollections = [];
        foreach ($dbs as $storeName)
        {
            $collections = $config->getIndexesGroupedByCollection($storeName);
            foreach ($collections as $collectionName=>$indexes)
            {
                // Don't do this for composites, which could be anywhere
                if(in_array($collectionName, array(TABLE_ROWS_COLLECTION,VIEWS_COLLECTION,SEARCH_INDEX_COLLECTION)))
                {
                    continue;
                }
                if ($reindex)
                {
                    $collection = $config->getCollectionForCBD($storeName, $collectionName);
                    if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                        $collection->dropIndexes();
                        $reindexedCollections[] = $collection->getNamespace();
                    }
                }
                foreach ($indexes as $indexName=>$fields)
                {
                    error_log('Index: '.$indexName.' - fields: '.json_encode($fields));
                    $indexName = substr($indexName,0,127); // ensure max 128 chars

                    $indexOptions = [
                        "background"=>$background
                    ];

                    if (!is_numeric($indexName))
                    {
                        $indexOptions['name'] = $indexName;
                    }

                    // New
                    //fields: [{"rdf:type.u":1},{"unique":true}]
                    // Old
                    //fields: {"_id":1,"_lockedForTrans":1}

                    $todoKeys = array_keys($fields);
                    if (is_numeric($todoKeys[0])) {
                        error_log('New nested format index '.json_encode($todoKeys[0]));
                        $indexFields = $fields[0];
                        error_log('field[1] '.json_encode($indexFields));
                        // TODO pass second array into options
                        $indexOptions = array_merge($indexOptions, $fields[1]);
                    } else {
                        error_log('Old format index '.json_encode($todoKeys[0]));
                        $indexFields = $fields;
                    }

                    error_log('create index - fields: '.json_encode($fields).' - options: '.json_encode($indexOptions));

                    $config->getCollectionForCBD($storeName, $collectionName)
                        ->createIndex(
                        $indexFields,
                        $indexOptions
                    );
        }
            }

            // Index views
            foreach($config->getViewSpecifications($storeName) as $viewId=>$spec)
            {
                $collection = $config->getCollectionForView($storeName, $viewId);
                if($collection)
                {
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1, _ID_KEY.'.'._ID_TYPE => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        ['value.'._IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
                    }
                    foreach($indexes as $index)
                    {
                        error_log('view index? '.json_encode($index));
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }

            // Index table rows
            foreach($config->getTableSpecifications($storeName) as $tableId=>$spec)
            {
                $collection = $config->getCollectionForTable($storeName, $tableId);
                if($collection)
                {
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1, _ID_KEY.'.'._ID_TYPE => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        ['value.'._IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
                    }
                    foreach($indexes as $index)
                    {
                        error_log('table row index? '.json_encode($index));
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }

            // index search documents
            foreach($config->getSearchDocumentSpecifications($storeName) as $searchId=>$spec)
            {
                $collection = $config->getCollectionForSearchDocument($storeName, $searchId);
                if($collection)
                {
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        [_IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];

                    if($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
                    }
                    foreach($indexes as $index)
                    {
                        error_log('search doc index? '.json_encode($index));
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }
        }
    }

    /**
     * returns mongo tripod config instance, this method aids helps with
     * testing.
     * @return \Tripod\Mongo\Config
     */
    protected function getConfig()
    {
        return \Tripod\Config::getInstance();
    }
}
