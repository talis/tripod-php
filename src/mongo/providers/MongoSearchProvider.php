<?php

namespace Tripod\Mongo;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';

use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use Tripod\Exceptions\SearchException;
use Tripod\ISearchProvider;
use Tripod\Timer;

class MongoSearchProvider implements ISearchProvider
{
    /**
     * @var string
     */
    protected $storeName;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var Driver
     */
    private $tripod;

    /**
     * @var Labeller
     */
    private $labeller;

    private $stopWords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours ', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'];

    public function __construct(Driver $tripod)
    {
        $this->tripod = $tripod;
        $this->storeName = $tripod->getStoreName();
        $this->labeller = new Labeller();
        $this->config = \Tripod\Config::getInstance();
    }

    /**
     * Indexes the given document.
     *
     * @param array $document the document to index
     *
     * @return mixed
     *
     * @throws SearchException if there was an error indexing the document
     */
    public function indexDocument($document)
    {
        if (isset($document['_id']['type'])) {
            $collection = $this->config->getCollectionForSearchDocument($this->storeName, $document['_id']['type']);
        } else {
            throw new SearchException('No search document type specified in document');
        }

        try {
            $result = $collection->updateOne(['_id' => $document['_id']], ['$set' => $document], ['upsert' => true]);
            if (!$result->isAcknowledged()) {
                throw new SearchException('Inserting search document not acknowledged');
            }
        } catch (\Exception $e) {
            throw new SearchException("Failed to Index Document \n" . print_r($document, true), 0, $e);
        }
    }

    /**
     * Removes a single document from the search index based on the specified resource and context and spec id.
     * If spec id is not specified this method will delete all search documents that match the resource and context.
     *
     * @param string            $resource
     * @param string            $context
     * @param array|string|null $specId
     *
     * @return mixed
     *
     * @throws SearchException if there was an error removing the document
     */
    public function deleteDocument($resource, $context, $specId = [])
    {
        $query = [_ID_KEY . '.' . _ID_RESOURCE => $this->labeller->uri_to_alias($resource), _ID_KEY . '.' . _ID_CONTEXT => $context];

        try {
            $searchTypes = [];
            if (!empty($specId)) {
                $specTypes = $this->config->getSearchDocumentSpecifications($this->storeName, null, true);
                if (is_string($specId)) {
                    if (!in_array($specId, $specTypes)) {
                        return;
                    }
                    $query[_ID_KEY][_ID_TYPE] = $specId;
                    $searchTypes[] = $specId;
                } elseif (is_array($specId)) {
                    // Only filter on search document spec types
                    $specId = array_intersect($specTypes, $specId);
                    if (empty($specId)) {
                        return;
                    }
                    $query[_ID_KEY . '.' . _ID_TYPE] = ['$in' => array_values($specId)];
                    $searchTypes = $specId;
                }
            }
            foreach ($this->config->getCollectionsForSearch($this->storeName, $searchTypes) as $collection) {
                $collection->deleteMany($query);
            }
        } catch (\Exception $e) {
            throw new SearchException("Failed to Remove Document with id \n" . print_r($query, true), 0, $e);
        }
    }

    /**
     * Returns the ids of all documents that contain and impact index entry
     * matching the resource and context specified.
     *
     * @param string $context
     *
     * @return array the ids of search documents that had matching entries in their impact index
     */
    public function findImpactedDocuments(array $resourcesAndPredicates, $context)
    {
        $contextAlias = $this->labeller->uri_to_alias($context);

        $specPredicates = [];

        foreach ($this->config->getSearchDocumentSpecifications($this->storeName) as $spec) {
            if (isset($spec[_ID_KEY])) {
                $specPredicates[$spec[_ID_KEY]] = $this->config->getDefinedPredicatesInSpec($this->storeName, $spec[_ID_KEY]);
            }
        }

        // build a filter - will be used for impactIndex detection and finding search types to re-gen
        $searchDocFilters = [];
        $resourceFilters = [];
        foreach ($resourcesAndPredicates as $resource => $resourcePredicates) {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $id = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];
            // If we don't have a working config or there are no predicates listed, remove all
            // rows associated with the resource in all search types
            if (empty($specPredicates) || empty($resourcePredicates)) {
                // build $filter for queries to impact index
                $resourceFilters[] = $id;
            } else {
                foreach ($specPredicates as $searchDocType => $predicates) {
                    // Only look for search rows if the changed predicates are actually defined in the searchDocspec
                    if (array_intersect($resourcePredicates, $predicates)) {
                        if (!isset($searchDocFilters[$searchDocType])) {
                            $searchDocFilters[$searchDocType] = [];
                        }
                        // build $filter for queries to impact index
                        $searchDocFilters[$searchDocType][] = $id;
                    }
                }
            }
        }

        $searchTypes = [];
        if (empty($searchDocFilters) && !empty($resourceFilters)) {
            $query = [_IMPACT_INDEX => ['$in' => $resourceFilters]];
        } else {
            $query = [];
            foreach ($searchDocFilters as $searchDocType => $filters) {
                // first re-gen views where resources appear in the impact index
                $query[] = [_IMPACT_INDEX => ['$in' => $filters], '_id.' . _ID_TYPE => $searchDocType];
                $searchTypes[] = $searchDocType;
            }

            if (!empty($resourceFilters)) {
                $query[] = [_IMPACT_INDEX => ['$in' => $resourceFilters]];
            }

            if (count($query) === 1) {
                $query = $query[0];
            } elseif (count($query) > 1) {
                $query = ['$or' => $query];
            }
        }
        if (empty($query)) {
            return [];
        }

        $searchDocs = [];
        foreach ($this->config->getCollectionsForSearch($this->storeName, $searchTypes) as $collection) {
            $cursor = $collection->find($query, ['projection' => ['_id' => true]]);
            foreach ($cursor as $d) {
                $searchDocs[] = $d;
            }
        }

        return $searchDocs;
    }

    /**
     * @param string $q
     * @param string $type
     * @param array  $indices
     * @param array  $fields
     * @param int    $limit
     * @param int    $offset
     *
     * @return array|mixed
     *
     * @throws SearchException
     */
    public function search($q, $type, $indices = [], $fields = [], $limit = 10, $offset = 0)
    {
        if (empty($q)) {
            throw new SearchException('You must specify a query');
        }
        if (empty($type)) {
            throw new SearchException('You must specify the search document type to restrict the query to');
        }
        if (empty($indices)) {
            throw new SearchException('You must specify at least one index from the search document specification to query against');
        }
        if (empty($fields)) {
            throw new SearchException('You must specify at least one field from the search document specification to return');
        }

        if (!is_numeric($limit) || $limit < 0) {
            throw new SearchException('Value for limit must be a positive number');
        }

        if (!is_numeric($offset) || $offset < 0) {
            throw new SearchException('Value for offset must be a positive number');
        }

        $original_terms = explode(' ', trim(strtolower($q)));
        $terms = array_values(array_diff($original_terms, $this->stopWords));

        // todo: this means if all the words entered were stop words, then use the orginal terms rather than do nothing!
        if (empty($terms)) {
            $terms = $original_terms;
        }

        $regexes = [];
        foreach ($terms as $t) {
            $regexes[] = new Regex("{$t}", '');
        }

        $query = [];
        $query['_id.type'] = $type;

        if (count($indices) == 1) {
            $searchIndex = $indices[0];
            $query[$searchIndex] = ['$all' => $regexes];
        } else {
            $query['$or'] = [];
            foreach ($indices as $searchIndex) {
                $query['$or'][] = ["{$searchIndex}" => ['$all' => $regexes]];
            }
        }

        $fieldsToReturn = [];
        foreach ($fields as $field) {
            $fieldsToReturn[$field] = 1;
        }
        $searchTimer = new Timer();
        $searchTimer->start();
        $cursor = $this->config->getCollectionForSearchDocument($this->storeName, $type)
            ->find($query, [
                'projection' => $fieldsToReturn,
                'limit' => $limit,
                'skip' => $offset]);

        $searchResults = [];
        $searchResults['head'] = [];
        $searchResults['head']['count'] = '';
        $searchResults['head']['limit'] = $limit;
        $searchResults['head']['offset'] = $offset;
        $searchResults['head']['duration'] = '';
        $searchResults['head']['query'] = $q;
        $searchResults['head']['query_terms_used'] = $terms;
        $searchResults['results'] = [];

        $count = $this->config->getCollectionForSearchDocument($this->storeName, $type)->count($query);
        if ($count > 0) {
            $searchResults['head']['count'] = $count;

            foreach ($cursor as $result) {
                // if more than one field has been asked for we need to
                // enumerate them in the results returned. However if only one has been
                // asked for then results is just set to that single fields value.
                if (count($fields) > 1) {
                    $r = [];
                    foreach ($fields as $field) {
                        if (isset($result[$field])) {
                            $r[$field] = $result[$field];
                        } else {
                            $r[$field] = '';
                        }
                    }
                    $searchResults['results'][] = $r;
                } else {
                    $searchResults['results'][] = $result[$fields[0]];
                }
            }
        } else {
            $searchResults['head']['count'] = 0;
        }
        $searchTimer->stop();
        $searchResults['head']['duration'] = $searchTimer->result() . ' ms';

        return $searchResults;
    }

    /**
     * @return string
     */
    public function getSearchCollectionName()
    {
        return SEARCH_INDEX_COLLECTION;
    }

    /**
     * Removes all documents from search index based on the specified type id.
     * Here search type id represents to id from, mongo tripod config, that is converted to _id.type in SEARCH_INDEX_COLLECTION
     * If type id is not specified this method will throw an exception.
     *
     * @param string           $typeId    Search type id
     * @param UTCDateTime|null $timestamp Optional timestamp to delete all search docs that are older than
     *
     * @return int The number of search documents deleted
     *
     * @throws \Tripod\Exceptions\Exception if there was an error performing the operation
     */
    public function deleteSearchDocumentsByTypeId($typeId, $timestamp = null)
    {
        $searchSpec = $this->getSearchDocumentSpecification($typeId);
        if ($searchSpec == null) {
            throw new SearchException("Could not find a search specification for {$typeId}");
        }
        $query = ['_id.type' => $typeId];
        if ($timestamp) {
            if (!$timestamp instanceof UTCDateTime) {
                $timestamp = new UTCDateTime($timestamp);
            }
            $query['$or'] = [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ];
        }
        $deleteResponse = $this->getCollectionForSearchSpec($typeId)
            ->deleteMany($query);

        return $deleteResponse->getDeletedCount();
    }

    /**
     * Count the number of documents in the spec that match $filters.
     *
     * @param string $searchSpec Search spec ID
     * @param array  $filters    Query filters to get count on
     *
     * @return int
     */
    public function count($searchSpec, array $filters = [])
    {
        $filters['_id.type'] = $searchSpec;

        return $this->getCollectionForSearchSpec($searchSpec)->count($filters);
    }

    /**
     * Returns the search document specification for the supplied type.
     *
     * @param string $typeId
     *
     * @return array|null
     */
    protected function getSearchDocumentSpecification($typeId)
    {
        return $this->config->getSearchDocumentSpecification($this->storeName, $typeId);
    }

    /**
     * For mocking.
     *
     * @param string $searchSpecId Search spec ID
     *
     * @return Collection
     */
    protected function getCollectionForSearchSpec($searchSpecId)
    {
        return $this->config->getCollectionForSearchDocument($this->storeName, $searchSpecId);
    }
}
