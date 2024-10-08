<?php

class MongoSearchProviderTest extends MongoTripodTestBase
{
    /** @var Tripod\Mongo\Composites\SearchIndexer */
    private $indexer;

    /** @var Tripod\Mongo\MongoSearchProvider */
    private $searchProvider;

    protected function setUp(): void
    {
        parent::setUp();

        $this->tripodTransactionLog = new Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->indexer = new Tripod\Mongo\Composites\SearchIndexer($this->tripod);
        $this->searchProvider = new Tripod\Mongo\MongoSearchProvider($this->tripod);
        $this->getTripodCollection($this->tripod)->drop();

        $this->loadBaseSearchDataViaTripod();

        foreach (Tripod\Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }

        // index all the documents
        $cursor = $this->getTripodCollection($this->tripod)
            ->find(['rdf:type.u' => ['$in' => ['bibo:Book']]], ['_id' => 1, 'rdf:type' => 1]); // ->limit(20);
        foreach ($cursor as $result) {
            $t = [];
            if (isset($result['rdf:type']['u'])) {
                $t[] = $result['rdf:type']['u'];
            } elseif (is_array($result['rdf:type'])) {
                foreach ($result['rdf:type'] as $_t) {
                    if (isset($_t['u'])) {
                        $t[] = $_t['u'];
                    }
                }
            }
            $this->indexer->generateAndIndexSearchDocuments($result['_id']['r'], $result['_id']['c'], $this->tripod->getPodName());
        }
    }

    public function testSearchIndexing()
    {

        // assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should have generated 13 search documents based on searchData.json');

        // define the expected search documents, this is what each of them should look like
        $expectedSearchDocs = [
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc1', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Physics for Engineers and Scientists', 'link' => 'http://talisaspire.com/resources/doc1', 'author' => 'Sayid Jarrah'],
                'search_terms' => ['physics for engineers and scientists', 'physics', 'science', 'sayid jarrah'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc1', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/1', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc2', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Astrophysics for Beginners', 'link' => 'http://talisaspire.com/resources/doc2', 'author' => 'Sayid Jarrah'],
                'search_terms' => ['astrophysics for beginners', 'astrophysics', 'science', 'sayid jarrah'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc2', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/1', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc3', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Fundementals of Heat and Mass Transfers', 'link' => 'http://talisaspire.com/resources/doc3', 'author' => 'Sayid Jarrah'],
                'search_terms' => ['fundementals of heat and mass transfers', 'engineering', 'science', 'sayid jarrah'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc3', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/1', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc4', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Paradise Lost', 'link' => 'http://talisaspire.com/resources/doc4', 'author' => 'John Locke'],
                'search_terms' => ['paradise lost', 'english literature', 'poetry', 'classics', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc4', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc5', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'The Divine Comedy', 'link' => 'http://talisaspire.com/resources/doc5', 'author' => 'John Locke'],
                'search_terms' => ['the divine comedy', "dante's the divine comedy", 'english literature', 'poetry', 'classics', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc5', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc6', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'A Light in the Attic', 'link' => 'http://talisaspire.com/resources/doc6', 'author' => 'John Locke'],
                'search_terms' => ['a light in the attic', 'english literature', 'poetry', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc6', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc7', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Revolting Rhymes', 'link' => 'http://talisaspire.com/resources/doc7', 'author' => 'John Locke'],
                'search_terms' => ['revolting rhymes', 'poetry', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc7', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc8', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'The Owl and the Pussycat', 'link' => 'http://talisaspire.com/resources/doc8', 'author' => 'John Locke'],
                'search_terms' => ['the owl and the pussycat', 'poetry', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc8', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc9', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'When We Were Very Young', 'link' => 'http://talisaspire.com/resources/doc9', 'author' => 'John Locke'],
                'search_terms' => ['when we were very young', 'poetry', 'john locke'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc9', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/2', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc10', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Love That Dog', 'link' => 'http://talisaspire.com/resources/doc10', 'author' => 'Jack Shephard'],
                'search_terms' => ['love that dog', 'poetry', 'jack shephard'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc10', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/3', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc11', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'Where the Sidewalk Ends', 'link' => 'http://talisaspire.com/resources/doc11', 'author' => 'Jack Shephard'],
                'search_terms' => ['where the sidewalk ends', 'poetry', 'jack shephard'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc11', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/3', 'c' => 'http://talisaspire.com/'],
                ],
            ],
            [
                '_id' => ['r' => 'http://talisaspire.com/resources/doc12', 'c' => 'http://talisaspire.com/', 'type' => 'i_search_resource'],
                'result' => ['title' => 'The Rime of the Ancient Mariner', 'link' => 'http://talisaspire.com/resources/doc12', 'author' => 'Jack Shephard'],
                'search_terms' => ['the rime of the ancient mariner', 'poetry', 'jack shephard'],
                '_impactIndex' => [
                    ['r' => 'http://talisaspire.com/resources/doc12', 'c' => 'http://talisaspire.com/'],
                    ['r' => 'http://talisaspire.com/authors/3', 'c' => 'http://talisaspire.com/'],
                ],
            ],
        ];

        // loop through every expected document and assert that it exists, and that each property matches the value we defined above.
        $searchCollection = Tripod\Config::getInstance()->getCollectionForSearchDocument($this->tripod->getStoreName(), 'i_search_resource');
        foreach ($expectedSearchDocs as $expectedSearchDoc) {
            $this->assertDocumentExists($expectedSearchDoc['_id'], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc['_id'], 'result', $expectedSearchDoc['result'], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc['_id'], 'search_terms', $expectedSearchDoc['search_terms'], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc['_id'], '_impactIndex', $expectedSearchDoc['_impactIndex'], $searchCollection);
        }
    }

    public function testSearchIndexingRemovesDocWhenTypeHasNoCorrespondingSearchdocSpec()
    {
        // update a document
        $id = ['_id.r' => 'http://talisaspire.com/resources/doc1'];
        $this->getTripodCollection($this->tripod)
            ->updateOne($id, ['$set' => ['rdf:type' => ['u' => 'bibo:Article']]]);

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(12, $actualSearchDocumentCount, 'Should only be 12 search documents now that one of them has had its type changed with no corresponding search doc spec');

        foreach (Tripod\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection) {
            $this->assertNull(
                $collection->findOne(['_id.r' => 'http://talisaspire.com/resources/doc1']),
                'No search document should be found, because it should have been deleted'
            );
        }

    }

    public function testSearchIndexingGeneratesNewDocForChangedTypeThatHasACorrespondingSearchdocSpec()
    {
        // update a document
        $id = ['_id.r' => 'http://talisaspire.com/resources/doc1'];

        $newData = [
            'rdf:type' => ['u' => 'resourcelist:List'],
            'spec:name' => ['l' => 'my list title'],
            'resourcelists:description' => ['l' => 'foo bar baz'],

        ];
        $this->getTripodCollection($this->tripod)->updateOne($id, ['$set' => $newData]);

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should only be 13 search documents');

        $result = [];
        foreach (Tripod\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection) {
            $result = $collection->findOne(['_id.r' => 'http://talisaspire.com/resources/doc1']);
            if ($result) {
                break;
            }
        }
        $this->assertEquals($result['_id'], [
            'r' => 'http://talisaspire.com/resources/doc1',
            'c' => 'http://talisaspire.com/',
            'type' => 'i_search_list',
        ]);
    }

    public function testSearchIndexingGeneratesTwoDocumentsForGivenResourceTheDeletesOneAfterFurtherUpdate()
    {
        // update a document
        $id = ['_id.r' => 'http://talisaspire.com/resources/doc1'];

        $newData = [
            'rdf:type' => [['u' => 'resourcelist:List'], ['u' => 'bibo:Book']],
            'spec:name' => ['l' => 'my list title'],
            'resourcelist:description' => ['l' => 'foo bar baz'],
        ];
        $this->getTripodCollection($this->tripod)->updateOne($id, ['$set' => $newData]);

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(14, $actualSearchDocumentCount, 'Should only be 14 search documents');

        $results = [];
        // We don't know where exactly these might have stored
        foreach (Tripod\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection) {
            foreach ($collection->find(['_id.r' => 'http://talisaspire.com/resources/doc1']) as $result) {
                if (!in_array($result, $results)) {
                    $results[] = $result;
                }
            }
        }

        $this->assertEquals(2, count($results));
        $expected = [
            [
                'r' => 'http://talisaspire.com/resources/doc1',
                'c' => 'http://talisaspire.com/',
                'type' => 'i_search_list',
            ],
            [
                'r' => 'http://talisaspire.com/resources/doc1',
                'c' => 'http://talisaspire.com/',
                'type' => 'i_search_resource',
            ],
        ];
        foreach ($results as $result) {
            $this->assertTrue(in_array(
                $result['_id'],
                $expected
            ));
        }

        // now update it again removing the resourcelist:List type
        $newData = [
            'rdf:type' => ['u' => 'bibo:Book'],
        ];
        $this->getTripodCollection($this->tripod)->updateOne($id, ['$set' => $newData]);

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should only be 13 search documents');

        $results = [];
        // We don't know where exactly these might have stored
        foreach (Tripod\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection) {
            foreach ($collection->find(['_id.r' => 'http://talisaspire.com/resources/doc1']) as $result) {
                if (!in_array($result, $results)) {
                    $results[] = $result;
                }
            }
        }

        $this->assertEquals(1, count($results));

        $result = array_pop($results);
        $this->assertEquals($result['_id'], [
            'r' => 'http://talisaspire.com/resources/doc1',
            'c' => 'http://talisaspire.com/',
            'type' => 'i_search_resource',
        ]);

    }

    public function testSearchThrowsExceptionIfNoQuery()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('You must specify a query');
        $this->searchProvider->search('', 'i_search_resource', ['search_terms'], ['result'], 3, 0);
    }

    public function testSearchThrowsExceptionIfNoType()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('You must specify the search document type to restrict the query to');
        $this->searchProvider->search('poetry', '', ['search_terms'], ['result'], 3, 0);
    }

    public function testSearchThrowsExceptionIfSearchIndicesEmpty()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('You must specify at least one index from the search document specification to query against');
        $this->searchProvider->search('poetry', 'i_search_resource', [], ['result'], 3, 0);
    }

    public function testSearchThrowsExceptionIfFieldsToReturnEmpty()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('You must specify at least one field from the search document specification to return');
        $this->searchProvider->search('poetry', 'i_search_resource', ['search_terms'], [], 3, 0);
    }

    public function testSearchThrowsExceptionIfLimitIsNegative()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('Value for limit must be a positive number');
        $this->searchProvider->search('poetry', 'i_search_resource', ['search_terms'], ['result'], -3, 0);
    }

    public function testSearchThrowsExceptionIfOffsetIsNegative()
    {
        $this->expectException(Tripod\Exceptions\SearchException::class);
        $this->expectExceptionMessage('Value for offset must be a positive number');
        $this->searchProvider->search('poetry', 'i_search_resource', ['search_terms'], ['result'], 3, -1);
    }

    public function testSearchLimitAndOffset()
    {
        $results = $this->searchProvider->search('poetry', 'i_search_resource', ['search_terms'], ['result'], 3, 0);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(0, $results['head']['offset']);

        $firstResult = $results['results'][0];
        $secondResult = $results['results'][1];
        $thirdResult = $results['results'][2];

        $results2 = $this->searchProvider->search('poetry', 'i_search_resource', ['search_terms'], ['result'], 3, 1);
        $this->assertEquals(9, $results2['head']['count']);
        $this->assertEquals(3, $results2['head']['limit']);
        $this->assertEquals(1, $results2['head']['offset']);

        $this->assertFalse(in_array($firstResult, $results2['results']));
        $this->assertEquals($secondResult, $results2['results'][0]);
        $this->assertEquals($thirdResult, $results2['results'][1]);
    }

    public function testSearchSingleIndex()
    {
        // simple search
        $results = $this->searchProvider->search('john locke poetry', 'i_search_resource', ['search_terms'], ['result'], 4, 0);
        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(4, $results['head']['limit']);
        $this->assertEquals(4, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('john locke poetry', $results['head']['query']);
        $this->assertEquals(['john', 'locke', 'poetry'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);

        $expectedResults = [
            ['title' => 'Paradise Lost', 'link' => 'http://talisaspire.com/resources/doc4', 'author' => 'John Locke'],
            ['title' => 'The Divine Comedy', 'link' => 'http://talisaspire.com/resources/doc5', 'author' => 'John Locke'],
            ['title' => 'A Light in the Attic', 'link' => 'http://talisaspire.com/resources/doc6', 'author' => 'John Locke'],
            ['title' => 'Revolting Rhymes', 'link' => 'http://talisaspire.com/resources/doc7', 'author' => 'John Locke'],
        ];

        $this->assertEquals($expectedResults, $results['results']);

        // search with some stop words
        $results = $this->searchProvider->search('the owl and the pussycat', 'i_search_resource', ['search_terms'], ['result'], 3, 0);
        $this->assertEquals(1, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(1, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('the owl and the pussycat', $results['head']['query']);
        $this->assertEquals(['owl', 'pussycat'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);

        // search returns no results
        $results = $this->searchProvider->search('october', 'i_search_resource', ['search_terms'], ['result'], 3, 0);
        $this->assertEquals(0, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(0, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('october', $results['head']['query']);
        $this->assertEquals(['october'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);
        $this->assertEquals([], $results['results']);

        // search single index but return multiple fields
        $results = $this->searchProvider->search('john locke poetry', 'i_search_resource', ['search_terms'], ['result', 'rdftype'], 3, 0);
        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('john locke poetry', $results['head']['query']);
        $this->assertEquals(['john', 'locke', 'poetry'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);

        $expectedResults = [
            ['result' => ['title' => 'Paradise Lost', 'link' => 'http://talisaspire.com/resources/doc4', 'author' => 'John Locke'], 'rdftype' => 'bibo:Book'],
            ['result' => ['title' => 'The Divine Comedy', 'link' => 'http://talisaspire.com/resources/doc5', 'author' => 'John Locke'], 'rdftype' => 'bibo:Book'],
            ['result' => ['title' => 'A Light in the Attic', 'link' => 'http://talisaspire.com/resources/doc6', 'author' => 'John Locke'], 'rdftype' => 'bibo:Book'],
        ];
        $this->assertEquals($expectedResults, $results['results']);

    }

    public function testSearchMultipleIndices()
    {
        $results = $this->searchProvider->search('bibo:Book', 'i_search_resource', ['search_terms', 'other_terms'], ['result', 'rdftype'], 3, 0);
        $this->assertEquals(13, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('bibo:Book', $results['head']['query']);
        $this->assertEquals(['bibo:book'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);

        $expectedResults = [
            ['result' => ['title' => 'Physics for Engineers and Scientists', 'link' => 'http://talisaspire.com/resources/doc1', 'author' => 'Sayid Jarrah'], 'rdftype' => 'bibo:Book'],
            ['result' => ['title' => 'Astrophysics for Beginners', 'link' => 'http://talisaspire.com/resources/doc2', 'author' => 'Sayid Jarrah'], 'rdftype' => 'bibo:Book'],
            ['result' => ['title' => 'Fundementals of Heat and Mass Transfers', 'link' => 'http://talisaspire.com/resources/doc3', 'author' => 'Sayid Jarrah'], 'rdftype' => 'bibo:Book'],
        ];

        $this->assertEquals($expectedResults, $results['results']);
    }

    public function testSearchWorksDirectlyFromTripod()
    {
        $results = $this->tripod->search([
            'q' => 'john locke poetry',
            'type' => 'i_search_resource',
            'indices' => ['search_terms'],
            'fields' => ['result'],
            'limit' => 3,
            'offset' => 0,
        ]);

        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals('john locke poetry', $results['head']['query']);
        $this->assertEquals(['john', 'locke', 'poetry'], $results['head']['query_terms_used']);
        $this->assertArrayHasKey('duration', $results['head']);

        $expectedResults = [
            ['title' => 'Paradise Lost', 'link' => 'http://talisaspire.com/resources/doc4', 'author' => 'John Locke'],
            ['title' => 'The Divine Comedy', 'link' => 'http://talisaspire.com/resources/doc5', 'author' => 'John Locke'],
            ['title' => 'A Light in the Attic', 'link' => 'http://talisaspire.com/resources/doc6', 'author' => 'John Locke'],
        ];

        $this->assertEquals($expectedResults, $results['results']);

    }

    public function testDeleteSearchDocumentsByTypeIdThrowsExceptionForInvalidType()
    {
        $mockSearchProvider = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();
        $mockSearchProvider->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_some_type')
            ->will($this->returnValue(null));

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Could not find a search specification for i_some_type');
        $mockSearchProvider->deleteSearchDocumentsByTypeId('i_some_type');
    }

    public function testDeleteSearchDocumentsByTypeIdDeletesNothingWhenNoMatchFound()
    {
        // first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should have generated 12 search documents based on searchData.json');

        $mockSearchProvider = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();
        $mockSearchProvider->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_some_type')
            ->will($this->returnValue(['i_some_type' => []]));
        try {
            $mockSearchProvider->deleteSearchDocumentsByTypeId('i_some_type');
        } catch (Tripod\Exceptions\ConfigException $e) {
            $this->assertEquals("Search document id 'i_some_type' not in configuration for store 'tripod_php_testing'", $e->getMessage());
        }

        // search document count should remain same, because we expect that there was nothing to delete
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $newSearchDocumentCount, 'Should have generated 12 search documents, because there was no match to remove');
    }

    public function testDeleteSearchDocumentsByTypeIdDeleteAllMatchingDocuments()
    {
        // first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should have generated 12 search documents based on searchData.json');

        $mockSearchProvider = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();
        $mockSearchProvider->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_search_resource')
            ->will($this->returnValue(['i_search_resource' => []]));

        $mockSearchProvider->deleteSearchDocumentsByTypeId('i_search_resource');

        // search document count should be 0, because we expect that everything should be deleted
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(0, $newSearchDocumentCount, 'Should have 0 search documents after removing all matching documents');
    }

    public function testDeleteSearchDocumentsByTypeIdDoNotDeleteNonMatchingDocuments()
    {
        // first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, 'Should have generated 13 search documents based on searchData.json');

        $id = ['_id.r' => 'http://talisaspire.com/resources/doc1'];

        $newData = [
            'rdf:type' => [['u' => 'resourcelist:List'], ['u' => 'bibo:Book']],
            'spec:name' => ['l' => 'my list title'],
            'resourcelist:description' => ['l' => 'foo bar baz'],
        ];
        $this->getTripodCollection($this->tripod)->updateOne($id, ['$set' => $newData]);

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        // assert that there are now 13 documents after adding new document to collection
        $updatedSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(14, $updatedSearchDocumentCount, 'Should have generated 14 search documents after adding a new document to collection');

        $mockSearchProvider = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();
        $mockSearchProvider->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_search_resource')
            ->will($this->returnValue(['i_search_resource' => []]));

        $mockSearchProvider->deleteSearchDocumentsByTypeId('i_search_resource');

        // search document count should be 1, since there is one document not matching the type id provided for delete
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(1, $newSearchDocumentCount, "Should have 1 search documents since there is one search document with 'i_search_list' type that does not match delete type.");
    }

    public function testCountSearchDocuments()
    {
        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $search = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getCollectionForSearchSpec'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $search->expects($this->once())
            ->method('getCollectionForSearchSpec')
            ->with('i_search_list')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with(['_id.type' => 'i_search_list'])
            ->will($this->returnValue(21));

        $this->assertEquals(21, $search->count('i_search_list'));
    }

    public function testCountSearchDocumentsWithFilters()
    {
        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $filters = ['_cts' => ['$lte' => new MongoDB\BSON\UTCDateTime(null)]];
        $query = array_merge(['_id.type' => 'i_search_list'], $filters);
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $search = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getCollectionForSearchSpec'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $search->expects($this->once())
            ->method('getCollectionForSearchSpec')
            ->with('i_search_list')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with($query)
            ->will($this->returnValue(89));

        $this->assertEquals(89, $search->count('i_search_list', $filters));
    }

    public function testDeleteSearchDocumentsBySearchId()
    {
        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(MongoDB\DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->will($this->returnValue(9));

        $search = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getCollectionForSearchSpec', 'getSearchDocumentSpecification'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $search->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_search_list')
            ->will($this->returnValue(['_id' => 'i_search_list']));

        $search->expects($this->once())
            ->method('getCollectionForSearchSpec')
            ->with('i_search_list')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with(['_id.type' => 'i_search_list'])
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(9, $search->deleteSearchDocumentsByTypeId('i_search_list'));
    }

    public function testDeleteSearchDocumentsBySearchIdWithTimestamp()
    {
        $timestamp = new MongoDB\BSON\UTCDateTime(null);

        $query = [
            '_id.type' => 'i_search_list',
            '$or' => [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ],
        ];

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(MongoDB\DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->will($this->returnValue(9));

        $search = $this->getMockBuilder(Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(['getCollectionForSearchSpec', 'getSearchDocumentSpecification'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $search->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->with('i_search_list')
            ->will($this->returnValue(['_id' => 'i_search_list']));

        $search->expects($this->once())
            ->method('getCollectionForSearchSpec')
            ->with('i_search_list')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with($query)
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(9, $search->deleteSearchDocumentsByTypeId('i_search_list', $timestamp));
    }

    /**
     * @param Tripod\Mongo\Driver $tripod
     * @param array $specs
     * @return int
     */
    protected function getCountForSearchSpecs(Tripod\Mongo\Driver $tripod, $specs = [])
    {
        $count = 0;
        if (empty($specs)) {
            $specs = Tripod\Config::getInstance()->getSearchDocumentSpecifications($tripod->getStoreName(), null, true);
        }

        foreach ($specs as $spec) {
            $count += Tripod\Config::getInstance()->getCollectionForSearchDocument($tripod->getStoreName(), $spec)->count(['_id.type' => $spec]);
        }
        return $count;
    }
}
