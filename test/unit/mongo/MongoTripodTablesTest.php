<?php

class MongoTripodTablesTest extends MongoTripodTestBase
{
    /**
     * @var Tripod\Mongo\Driver
     */
    protected $tripod;

    /**
     * @var Tripod\Mongo\TransactionLog
     */
    protected $tripodTransationLog;

    /**
     * @var Tripod\Mongo\Composites\Tables
     */
    protected $tripodTables;

    private $tablesConstParams;

    protected $defaultContext = 'http://talisaspire.com/';

    protected $defaultStoreName = 'tripod_php_testing';

    protected $defaultPodName = 'CBD_testing';

    protected function setUp(): void
    {
        parent::setup();

        $this->tripodTransactionLog = new Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Tripod\Mongo\Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            ['async' => [OP_VIEWS => false, OP_TABLES => false, OP_SEARCH => false]]
        );

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);
        $this->loadResourceDataViaTripod();
        $this->tablesConstParams = [
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/',
        ];

        $this->tripodTables = new Tripod\Mongo\Composites\Tables(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            null // pass null context, should default to http://talisaspire.com
        );

        // purge tables
        foreach (Tripod\Config::getInstance()->getCollectionsForTables($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }
    }

    /**
     * Generate dummy config that we can use for creating a Config object
     * @return array
     */
    private function generateMongoTripodTestConfig()
    {
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'mongodb://localhost',
            ],
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018',
            ],
        ];
        $config['stores'] = [
            $this->defaultStoreName => [
                'data_source' => 'db',
                'pods' => [
                    $this->defaultPodName => [],
                ],
            ],
        ];
        $config['queue'] = ['database' => 'queue', 'collection' => 'q_queue', 'data_source' => 'db'];
        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'db',
        ];
        return $config;
    }

    /**
     * Generate table rows based off an id
     * @param string $id
     * @return array
     */
    private function generateTableRows($id)
    {
        $this->tripodTables->generateTableRows($id);
        return $this->tripodTables->getTableRows($id);
    }

    public function testTripodSaveChangesUpdatesLiteralTripleInTable()
    {
        $this->tripodTables->generateTableRows('t_resource', 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2');

        $t1 = $this->tripodTables->getTableRows('t_resource', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $expectedIsbn13s = ['9780393929691', '9780393929691-2'];
        $this->assertEquals($expectedIsbn13s, $t1['results'][0]['isbn13']);

        $g1 = $this->tripod->describeResource('http://talisaspire.com/works/4d101f63c10a6');
        $g2 = $this->tripod->describeResource('http://talisaspire.com/works/4d101f63c10a6');

        $g2->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6', $g2->qname_to_uri('bibo:isbn13'), '9780393929691-3');
        $this->tripod->saveChanges($g1, $g2, 'http://talisaspire.com/');

        $t2 = $this->tripodTables->getTableRows('t_resource', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $expectedIsbn13s = ['9780393929691', '9780393929691-2', '9780393929691-3'];
        $this->assertEquals($expectedIsbn13s, $t2['results'][0]['isbn13']);
    }

    public function testGenerateTableRowsWithCounts()
    {
        $this->tripodTables->generateTableRows('t_source_count');

        $t1 = $this->tripodTables->getTableRows('t_source_count');

        // expecting two rows
        $this->assertEquals(count($t1['results']), 3);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertEquals(0, $result['random_predicate_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows('t_source_count');

        $t1 = $this->tripodTables->getTableRows('t_source_count');

        // expecting two rows
        $this->assertEquals(count($t1['results']), 3);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new Tripod\ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://example.com');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_source_count');

        $result = null;
        $this->assertEquals(count($t2['results']), 3);
        foreach ($t2['results'] as $r) {
            if ($r['_id']['r'] == $subject) {
                $result = $r;
            }
        }

        $this->assertNotNull($result, "Cound not find table row for {$subject}");
        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(2, $result['source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountAndRegexUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows('t_source_count_regex');

        $t1 = $this->tripodTables->getTableRows('t_source_count_regex');

        // expecting two rows
        $this->assertEquals(count($t1['results']), 3);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertEquals(0, $result['regex_source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new Tripod\ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://foobarbaz.com');
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://example.com/foobarbaz');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_source_count_regex');

        $result = null;
        $this->assertEquals(count($t2['results']), 3);
        foreach ($t2['results'] as $r) {
            if ($r['_id']['r'] == $subject) {
                $result = $r;
            }
        }

        $this->assertNotNull($result, "Could not find table row for {$subject}");
        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(3, $result['source_count']);
        $this->assertEquals(2, $result['regex_source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountOnJoinAndRegexUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows('t_join_source_count_regex');

        $t1 = $this->tripodTables->getTableRows('t_join_source_count_regex', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        // expecting two rows
        $this->assertEquals(count($t1['results']), 1);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count', $result, 'Result does not contain titles_count');
        $this->assertEquals(3, $result['titles_count']);

        // add a title to f340...
        $subjectGraph = $this->tripod->describeResource('http://jacs3.dataincubator.org/f340');
        $newGraph = new Tripod\ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple('http://jacs3.dataincubator.org/f340', 'http://purl.org/dc/terms/title', 'Another title');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_join_source_count_regex', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $this->assertEquals(count($t2['results']), 1);
        $result = $t2['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count', $result, 'Result does not contain titles_count');
        $this->assertEquals(4, $result['titles_count']);
    }

    public function testUpdateWillDeleteItem()
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['deleteTableRowsForResource', 'generateTableRowsForType'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();
        $mockTables->expects($this->once())->method('deleteTableRowsForResource')->with('http://foo', 'context');
        $mockTables->expects($this->never())->method('generateTableRowsForType');

        $mockTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'http://foo', 'c' => 'context'], OP_TABLES, 'foo', 'bar', ['t_table']));
    }

    public function testUpdateWillGenerateRows()
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['deleteTableRowsForResource', 'generateTableRowsForResource'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();
        $mockTables->expects($this->once())->method('generateTableRowsForResource')->with('http://foo', 'context');
        $mockTables->expects($this->never())->method('deleteTableRowsForResource');

        $mockTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'http://foo', 'c' => 'context'], OP_TABLES, 'foo', 'bar', ['t_table']));
    }

    public function testGenerateTableRows()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource');

        // expecting two rows
        $this->assertEquals(count($t1['results']), 3);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertTrue(isset($result['type']), 'Result does not contain type');
        $this->assertTrue(isset($result['isbn']), 'Result does not contain isbn');
        $this->assertTrue(isset($result['isbn13']), 'Result does not contain isbn13');
    }

    public function testBatchTableRowGeneration()
    {
        $count = 234;
        $docs = [];

        $configOptions = json_decode(file_get_contents(__DIR__ . '/data/config.json'), true);

        for ($i = 0; $i < $count; $i++) {
            $docs[] = ['_id' => ['r' => 'tenantLists:batch' . $i, 'c' => 'tenantContexts:DefaultGraph']];
        }

        $fakeCursor = new ArrayIterator($docs);
        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForTable', 'getCollectionForCBD'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig($configOptions);

        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->onlyMethods(['count', 'find'])
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->getMock();
        $collection->expects($this->atLeastOnce())->method('count')->willReturn($count);
        $collection->expects($this->atLeastOnce())->method('find')->willReturn($fakeCursor);

        $configInstance->expects($this->atLeastOnce())->method('getCollectionForCBD')->willReturn($collection);

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getConfigInstance', 'queueApplyJob'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'tenantContexts:DefaultGraph'])
            ->getMock();
        $tables->expects($this->atLeastOnce())->method('getConfigInstance')->willReturn($configInstance);
        $tables->expects($this->exactly(3))->method('queueApplyJob')
            ->withConsecutive(
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(34)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ]
            );
        $tables->generateTableRows('t_resource', null, null, 'TESTQUEUE');
    }

    public function testGetTableRowsSort()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => -1, '_id.r' => 1]);
        // expecting two rows, first row should be one with highest numeric value of ISBN, due to sort DESC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2', $t1['results'][0]['_id']['r']);

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1, '_id.r' => 1]);

        // expecting two rows, first row should be one with lowest numeric value of ISBN, due to sort ASC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsFilter()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', ['value.isbn' => '9780393929690']); // only bring back rows with isbn = 9780393929690

        // expecting one row
        $this->assertTrue(count($t1['results']) == 1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsLimitOffset()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1], 0, 1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA
        $this->assertTrue(count($t1['results']) == 1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);

        $t2 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1], 1, 1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2
        $this->assertTrue(count($t2['results']) == 1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2', $t2['results'][0]['_id']['r']);
    }

    public function testGenerateTableRowsForResourceUnnamespaced()
    {
        $this->tripodTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'http://basedata.com/b/2', 'c' => 'http://basedata.com/b/DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');
    }

    public function testGenerateTableRowsForResourceNamespaced()
    {
        $this->tripodTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'baseData:2', 'c' => 'baseData:DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');
    }

    public function testGenerateTableRowsForResourceContextNamespaced()
    {
        $this->tripodTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'http://basedata.com/b/2', 'c' => 'baseData:DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');
    }

    public function testGenerateTableRowsForResourceResourceNamespaced()
    {
        $this->tripodTables->update(new Tripod\Mongo\ImpactedSubject(['r' => 'baseData:2', 'c' => 'http://basedata.com/b/DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');
    }

    public function testGenerateTableRowsForResourcesOfTypeWithNamespace()
    {
        $mockTripodTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->will($this->returnValue(['ok' => true]));

        // check where referred to as acorn:Work2 in spec...
        $mockTripodTables->generateTableRowsForType('http://talisaspire.com/schema#Work2');

        $mockTripodTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->will($this->returnValue(['ok' => true]));

        // check where referred to as http://talisaspire.com/schema#Resource in spec...
        $mockTripodTables->generateTableRowsForType('acorn:Resource');
    }

    /**
     * Test table specification predicate modifier config
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersValidConfig()
    {
        $this->expectNotToPerformAssertions();

        // All config defined here should be valid
        $tableSpecifications = [
            _ID_KEY => 't_testGenerateTableRowsForUsersWithModifiersValidConfig',
            'fields' => [
                [
                    'fieldName' => 'test1',
                    'predicates' => [
                        'join' => [
                            'glue' => ';',
                            'predicates' => ['foaf:name'],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test2',
                    'predicates' => [
                        'lowercase' => [
                            'predicates' => ['foaf:name'],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test3',
                    'predicates' => [
                        'lowercase' => [
                            'join' => [
                                'glue' => ';',
                                'predicates' => ['foaf:name'],
                            ],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test4',
                    'predicates' => [
                        'date' => [
                            'predicates' => ['temp:last_login'],
                        ],
                    ],
                ],
            ],
        ];

        // Note that you need some config in order to create the Config object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        Tripod\Config::setConfig($this->generateMongoTripodTestConfig());
        /** @var Tripod\Mongo\Config */
        $tripodConfig = Tripod\Config::getInstance();

        foreach ($tableSpecifications['fields'] as $field) {
            // If there is invalid config, an exception will be thrown
            $tripodConfig->checkModifierFunctions($field['predicates'], Tripod\Mongo\Composites\Tables::$predicateModifiers);
        }

    }

    /**
     * Test invalid table specification predicate modifier config - use a bad attribute
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersInvalidConfigBadGlue()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Invalid modifier: 'glue2' in key 'join'");

        // Create some dodgy config ("glue2") and see if an exception is thrown
        $tableSpecifications = [
            _ID_KEY => 't_foo',
            'fieldName' => 'test1',
            'predicates' => [
                'join' => [
                    'glue2' => ';',
                    'predicates' => ['foaf:name'],
                ],
            ],
        ];

        // Note that you need some config in order to create the Config object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        Tripod\Config::setConfig($this->generateMongoTripodTestConfig());
        /** @var Tripod\Mongo\Config */
        $tripodConfig = Tripod\Config::getInstance();

        $tripodConfig->checkModifierFunctions($tableSpecifications['predicates'], Tripod\Mongo\Composites\Tables::$predicateModifiers);
    }

    /**
     * Test table rows have been generated successfully for a "join" modifier
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoin()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        $this->assertEquals('Harry Potter', $rows['results'][0]['join']);
    }

    /**
     * Test table rows have been generated for a "join" modifier but with a single value rather than an array
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinSingle()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        $this->assertEquals('Harry', $rows['results'][0]['joinSingle']);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier with a "join" inside it
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCase()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        $this->assertEquals('harry potter', $rows['results'][0]['joinLowerCase']);
    }

    /**
     * Test table rows have been generated for a "date" modifier
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDate()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $rows['results'][0]['mongoDate']);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with a value that does not exist
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDateDoesNotExist()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        // Test for data that doesn't exist
        $this->assertArrayNotHasKey('mongoDateDoesNotExist', $rows['results'][0]);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier wtih a "join" modifier inside. It also has an
     * extra field attached to the row as well
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCaseAndExtraField()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        $this->assertArrayHasKey('joinLowerCaseANDExtraField', $rows['results'][0]);
        $this->assertIsArray($rows['results'][0]['joinLowerCaseANDExtraField']);
        $this->assertEquals('harry potter', $rows['results'][0]['joinLowerCaseANDExtraField'][0]);
        $this->assertEquals('Harry', $rows['results'][0]['joinLowerCaseANDExtraField'][1]);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with an invalid date string
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersDateInvalid()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        // Check borked data
        // Trying to use date but passed in a string - should default to 0 for sec and usec
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $rows['results'][0]['mongoDateInvalid']);
        $this->assertEquals(0, $rows['results'][0]['mongoDateInvalid']->__toString());
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier around a "date" modifier
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersLowercaseDate()
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows['head']['count'] == 1, 'Expected one row');

        // Lowercasing a mongodate object should be the same as running a __toString() on the date object
        $this->assertEquals($rows['results'][0]['mongoDate']->__toString(), $rows['results'][0]['lowercaseDate']);
    }

    /**
     * Test table rows are tuncated if they are too large to index
     * @return void
     */
    public function testGenerateTableRowsTruncatesFieldsTooLargeToIndex()
    {
        $fullTitle = 'Mahommah Gardo Baquaqua. Biography of Mahommah G. Baquaqua, a Native of Zoogoo, in the Interior of Africa. (A Convert to Christianity,) With a Description of That Part of the World; Including the Manners and Customs of the Inhabitants, Their Religious Notions, Form of Government, Laws, Appearance of the Country, Buildings, Agriculture, Manufactures, Shepherds and Herdsmen, Domestic Animals, Marriage Ceremonials, Funeral Services, Styles of Dress, Trade and Commerce, Modes of Warfare, System of Slavery, &amp;c., &amp;c. Mahommah&#039;s Early Life, His Education, His Capture and Slavery in Western Africa and Brazil, His Escape to the United States, from Thence to Hayti, (the City of Port Au Prince,) His Reception by the Baptist Missionary There, The Rev. W. L. Judd; His Conversion to Christianity, Baptism, and Return to This Country, His Views, Objects and Aim. Written and Revised from His Own Words, by Samuel Moore, Esq., Late Publisher of the &quot;North of England Shipping Gazette,&quot; Author of Several Popular Works, and Editor of Sundry Reform Papers.';
        $truncatedTitle = substr($fullTitle, 0, 1007); // 1007 = 1024 - index name "value_title_1" + Randomness
        $fullTitleLength = strlen($fullTitle);
        $truncatedTitleLength = strlen($truncatedTitle);

        $rows = $this->generateTableRows('t_truncation');

        // When using Mongo 2.4 and below, the string will not have been truncated.
        // Due to stricter index key enforcement in Mongo 2.6 and above, the string will have been truncated.
        // Allow the test to pass for either version of Mongo
        $actualLength = strlen($rows['results'][0]['title']);
        $this->assertTrue($actualLength === $fullTitleLength || $actualLength === $truncatedTitleLength, 'Title is an unexpected length');

        // Assert that the title starts with the truncated title.
        // This will be the case for both Mongo 2.4 and Mongo 2.6
        $this->assertTrue(strpos($rows['results'][0]['title'], $truncatedTitle) === 0, 'Unexpected title');
    }

    /**
     * Test that link modifier is derived from the joined resource id, rather than base
     * @return void
     */
    public function testJoinLinkValueIsForJoinedResource()
    {
        $this->tripodTables->generateTableRows('t_join_link');
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:foo1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertArrayHasKey('knowsLink', $rows['results'][0]);
        $this->assertArrayHasKey('workLink', $rows['results'][0]);
        // Check bookLink values
        $this->assertEquals('baseData:foo1234', $rows['results'][0]['_id']['r']);
        $this->assertEquals('http://basedata.com/b/foo1234', $rows['results'][0]['bookLink']);

        // Check authorLink values
        $this->assertEquals('user:10101', $rows['results'][0]['authorUri']);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10101', $rows['results'][0]['authorLink']);

        // Check knowsLink values
        $this->assertEquals('user:10102', $rows['results'][0]['knowsUri']);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10102', $rows['results'][0]['knowsLink']);

        // Check workLink values
        $this->assertEquals('http://talisaspire.com/works/4d101f63c10a6', $rows['results'][0]['workUri']); // Already a fq URI
        $this->assertEquals('http://talisaspire.com/works/4d101f63c10a6', $rows['results'][0]['workLink']);
    }

    /**
     * Test to ensure that impact index contains joined ids for resources that do not yet exist in the database (i.e.
     * allow open world model)
     * @return void
     */
    public function testPreviouslyUnavailableDataBecomesPresentAndTriggersTableRegen()
    {
        $this->tripodTables->generateTableRows('t_join_link');
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:bar1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertEquals('user:10103', $rows['results'][0]['authorUri']);
        // Author link should not appear because resource has not yet been created
        $this->assertArrayNotHasKey('authorLink', $rows['results'][0]);

        $uri = 'http://schemas.talis.com/2005/user/schema#10103';
        // Confirm this user does not exist
        $this->assertFalse($this->tripod->describeResource($uri)->has_triples_about($uri));

        $g = new Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('foaf:Person'));
        $g->add_literal_triple($uri, $g->qname_to_uri('foaf:name'), 'A. Nonymous');
        $this->tripod->saveChanges(new Tripod\Mongo\MongoGraph(), $g, 'http://talisaspire.com/', "This resource didn't exist at join time");

        $userGraph = $this->tripod->describeResource($uri);

        $this->assertTrue($userGraph->has_triples_about($uri), 'new entity we created was not saved');

        // Get our table rows again
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:bar1234']);
        // authorLink should now be populated
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertEquals($uri, $rows['results'][0]['authorLink']);
    }

    /**
     * Ensure that an array of links is returned if there are multiple resources matched by the join
     * @return void
     */
    public function testLinkWorksOnRepeatingPredicatesForResource()
    {
        $this->tripodTables->generateTableRows('t_link_multiple');
        $rows = $this->tripodTables->getTableRows('t_link_multiple', ['_id.r' => 'baseData:bar1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('contributorLink', $rows['results'][0]);
        $this->assertTrue(is_array($rows['results'][0]['contributorLink']));
        $this->assertEquals(2, count($rows['results'][0]['contributorLink']));
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10101', $rows['results'][0]['contributorLink'][0]);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10102', $rows['results'][0]['contributorLink'][1]);
    }

    /**
     * Return the distinct values of a table column
     * @return void
     */
    public function testDistinct()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $rows = $this->tripodTables->getTableRows($table, [], [], 0, 0);
        $this->assertEquals(11, $rows['head']['count']);
        $results = $this->tripodTables->distinct($table, 'value.title');

        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(4, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(4, count($results['results']));
        $this->assertContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        // Supply a filter
        $results = $this->tripodTables->distinct($table, 'value.title', ['value.type' => 'bibo:Document']);
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(2, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(2, count($results['results']));
        $this->assertNotContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        $results = $this->tripodTables->distinct($table, 'value.type');
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(7, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(7, count($results['results']));
        $this->assertContains('acorn:Resource', $results['results']);
        $this->assertContains('acorn:Work', $results['results']);
        $this->assertContains('bibo:Book', $results['results']);
        $this->assertContains('bibo:Document', $results['results']);
    }

    /**
     * Return no results for tablespec that doesn't exist
     * @return void
     */
    public function testDistinctOnTableSpecThatDoesNotExist()
    {
        $table = 't_nothing_to_see_here';
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Table id \'t_nothing_to_see_here\' not in configuration');
        $results = $this->tripodTables->distinct($table, 'value.foo');
    }

    /**
     * Return no results for distinct on a fieldname that is not defined in tableSpec
     * @return void
     */
    public function testDistinctOnFieldNameThatIsNotInTableSpec()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, 'value.foo');
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**
     * Return no results for filters that match no table rows
     * @return void
     */
    public function testDistinctForFilterWithNoMatches()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, 'value.title', ['value.foo' => 'wibble']);
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    public function testTableRowsGenerateWhenDefinedPredicateChanges()
    {
        foreach (Tripod\Config::getInstance()->getTableSpecifications($this->tripod->getStoreName()) as $specId => $spec) {
            $this->generateTableRows($specId);
        }

        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2';

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $labeller = new Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:title'],
        ];

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([
                $tripod->getStoreName(),
                $this->getTripodCollection($tripod),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $tables->expects($this->exactly(2))
            ->method('generateTableRows')
            ->withConsecutive(
                [
                    $this->equalTo('t_distinct'),
                    $this->equalTo($uri),
                    $this->equalTo($this->defaultContext),
                ],
                [
                    $this->equalTo('t_join_source_count_regex'),
                    $this->equalTo($uri),
                    $this->equalTo($this->defaultContext),
                ]
            );

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        // Walk through the processSyncOperations process manually for tables

        /** @var Tripod\Mongo\Composites\Tables $table */
        $table = $tripod->getComposite(OP_TABLES);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Tables::class, $table);

        $expectedImpactedSubject = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => $labeller->uri_to_alias($uri),
                _ID_CONTEXT => $this->defaultContext,
            ],
            OP_TABLES,
            $this->defaultStoreName,
            $this->defaultPodName,
            ['t_distinct', 't_join_source_count_regex']
        );

        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $impactedSubject = $impactedSubjects[0];

        $this->assertEquals($expectedImpactedSubject->getResourceId(), $impactedSubject->getResourceId());
        $this->assertEquals($expectedImpactedSubject->getOperation(), $impactedSubject->getOperation());
        $this->assertEquals($expectedImpactedSubject->getStoreName(), $impactedSubject->getStoreName());
        $this->assertEquals($expectedImpactedSubject->getPodName(), $impactedSubject->getPodName());

        // Order of these doesn't matter - so sort the spec types for matching
        $expectedSpecTypes = $expectedImpactedSubject->getSpecTypes();
        sort($expectedSpecTypes);
        $specTypes = $impactedSubject->getSpecTypes();
        sort($specTypes);

        $this->assertEquals($expectedSpecTypes, $specTypes);

        foreach ($impactedSubjects as $subject) {
            $table->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated table.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        $collections = Tripod\Config::getInstance()->getCollectionsForTables($this->defaultStoreName);
        foreach ($collections as $collection) {
            $query = [
                'value._impactIndex' => ['r' => $labeller->uri_to_alias($uri), 'c' => $this->defaultContext],
                '_id.type' => ['$in' => ['t_distinct', 't_join_source_count_regex']],
            ];
            $this->assertEquals(0, $collection->count($query));
        }
    }

    public function testTableRowsNotGeneratedWhenUndefinedPredicateChanges()
    {
        foreach (Tripod\Config::getInstance()->getTableSpecifications($this->tripod->getStoreName()) as $specId => $spec) {
            $this->generateTableRows($specId);
        }

        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2';

        $labeller = new Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:description'],
        ];

        // Walk through the processSyncOperations process manually for tables

        $table = new Tripod\Mongo\Composites\Tables(
            $this->defaultStoreName,
            Tripod\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
            $this->defaultContext
        );

        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEmpty($impactedSubjects);
    }

    public function testUpdateOfResourceInImpactIndexTriggersRegenerationTableRows()
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();

        $mockTables->expects($this->exactly(2))
            ->method('generateTableRows')
            ->withConsecutive(
                [
                    't_resource',
                    'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                    $this->defaultContext,
                ],
                [
                    't_resource',
                    'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',
                    $this->defaultContext,
                ]
            );

        $labeller = new Tripod\Mongo\Labeller();
        // generate table rows
        $this->tripodTables->generateTableRows('t_resource');

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://talisaspire.com/works/4d101f63c10a6') => [
                'bibo:isbn13',
            ],
        ];

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_resource']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_resource']
            ),
        ];

        $impactedSubjects = $mockTables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        // next, trigger regen for work we know is associated with 2x resources. Should trigger view regen for resources
        foreach ($impactedSubjects as $subject) {
            $mockTables->update($subject);
        }
    }

    public function testRdfTypeTriggersGenerationOfTableRows()
    {
        $uri = 'http://example.com/resources/' . uniqid();

        $labeller = new Tripod\Mongo\Labeller();
        $graph = new Tripod\ExtendedGraph();
        // This should trigger a table row regeneration, even though issn isn't in the tablespec
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('bibo:issn'), '1234-5678');

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => [
                'rdf:type', 'bibo:issn',
            ],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRowsForType'])
            ->setConstructorArgs([
                $this->defaultStoreName,
                Tripod\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                $this->defaultContext,
            ])
            ->getMock();

        $mockTables->expects($this->once())
            ->method('generateTableRowsForType')
            ->with(
                'acorn:Resource',
                $labeller->uri_to_alias($uri),
                $this->defaultContext,
                []
            );

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri),
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                []
            ),
        ];

        $mockTripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $impactedSubjects = $mockTables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
        foreach ($impactedSubjects as $subject) {
            $mockTables->update($subject);
        }
    }

    public function testUpdateToResourceWithMatchingRdfTypeShouldOnlyRegenerateIfRdfTypeIsPartOfUpdate()
    {
        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA';
        $labeller = new Tripod\Mongo\Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $tables = new Tripod\Mongo\Composites\Tables(
            $this->defaultStoreName,
            Tripod\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
            $this->defaultContext
        );

        $subjectsAndPredicatesOfChange = [$uriAlias => ['dct:subject']];

        $this->assertEmpty($tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));

        $subjectsAndPredicatesOfChange = [$uriAlias => ['dct:subject', 'rdf:type']];

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                []
            ),
        ];

        $impactedSubjects = $tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testNewResourceThatDoesNotMatchAnythingCreatesNoImpactedSubjects()
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new Tripod\ExtendedGraph();
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('bibo:Proceedings'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('dct:title'), 'A title');

        $subjectsAndPredicatesOfChange = [$uriAlias => ['rdf:type', 'dct:title']];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $tables = $mockTripod->getComposite(OP_TABLES);

        $this->assertEmpty($tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

    public function testDeleteResourceCreatesImpactedSubjects()
    {
        $uri = 'http://example.com/users/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new Tripod\ExtendedGraph();
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('spec:User')
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('foaf:firstName'),
            'Anne'
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('foaf:surname'),
            'Onymous'
        );

        $uri2 = 'http://example.com/users/' . uniqid();
        $uriAlias2 = $labeller->uri_to_alias($uri2);

        $graph2 = new Tripod\ExtendedGraph();
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('spec:User')
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('foaf:firstName'),
            'Ann'
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('foaf:surname'),
            'O\'ther'
        );

        // Save the graphs and ensure that table rows are generated
        $tripod = new Tripod\Mongo\Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            [
                'defaultContext' => $this->defaultContext,
                OP_ASYNC => [
                    OP_VIEWS => false,
                    OP_TABLES => false,
                    OP_SEARCH => false,
                ],
            ]
        );

        $tripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(1, $tableRows['head']['count']);

        $tripod->saveChanges(new Tripod\ExtendedGraph(), $graph2);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias2,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(1, $tableRows['head']['count']);

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    $this->defaultPodName,
                    $this->defaultStoreName,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->setConstructorArgs(
                [
                    $mockTripod,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->onlyMethods(['processSyncOperations'])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $expectedSubjectsAndPredicatesOfChange = [
            $uriAlias => ['rdf:type', 'foaf:firstName', 'foaf:surname'],
            $uriAlias2 => ['rdf:type', 'foaf:firstName', 'foaf:surname'],
        ];

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $expectedSubjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $graph->add_graph($graph2);

        // Delete both user resources
        $mockTripod->saveChanges($graph, new Tripod\ExtendedGraph());

        $deletedGraph = $mockTripod->describeResources([$uri, $uri2]);

        $this->assertTrue($deletedGraph->is_empty());

        // Manually walk through the tables operation
        /** @var Tripod\Mongo\Composites\Tables $tables */
        $tables = $mockTripod->getComposite(OP_TABLES);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_users']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias2,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_users']
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $tables->getImpactedSubjects($expectedSubjectsAndPredicatesOfChange, $this->defaultContext));

        foreach ($expectedImpactedSubjects as $subject) {
            $tables->update($subject);
        }

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(0, $tableRows['head']['count']);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias2,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(0, $tableRows['head']['count']);
    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject()
    {
        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    'CBD_testing',
                    'tripod_php_testing',
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([])
            ->setConstructorArgs(
                [
                    $tripod,
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new Tripod\Mongo\MongoGraph();
        $newSubjectUri1 = 'http://talisaspire.com/resources/newdoc1';
        $newSubjectUri2 = 'http://talisaspire.com/resources/newdoc2';
        $newSubjectUri3 = 'http://talisaspire.com/resources/newdoc3';

        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Article')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:title'), 'This is a new resource');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'history');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'philosophy');

        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Book')); // this is the only resource that should be queued
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:title'), 'This is another new resource');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'maths');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'science');

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Journal')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:title'), 'This is yet another new resource');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'art');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'design');
        $subjectsAndPredicatesOfChange = [
            $newSubjectUri1 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri2 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri3 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
        ];
        $tripod->saveChanges(new Tripod\Mongo\MongoGraph(), $g);

        /** @var Tripod\Mongo\Composites\Tables $tables */
        $tables = $tripod->getComposite(OP_TABLES);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $newSubjectUri2,
                    _ID_CONTEXT => 'http://talisaspire.com/',
                ],
                OP_TABLES,
                'tripod_php_testing',
                'CBD_testing',
                []
            ),
        ];

        $impactedSubjects = $tables->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testRemoveTableSpecDoesNotAffectInvalidation()
    {
        foreach (Tripod\Config::getInstance()->getTableSpecifications($this->tripod->getStoreName()) as $specId => $spec) {
            $this->generateTableRows($specId);
        }

        $context = 'http://talisaspire.com/';
        $uri = 'http://talisaspire.com/works/4d101f63c10a6';

        $collection = Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_resource');
        $this->assertGreaterThan(0, $collection->count(['_id.type' => 't_resource', 'value._impactIndex' => [_ID_RESOURCE => $uri, _ID_CONTEXT => $context]]));
        $config = Tripod\Config::getConfig();
        unset($config['stores']['tripod_php_testing']['table_specifications'][0]);
        Tripod\Config::setConfig($config);

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(
                [
                    'CBD_testing',
                    'tripod_php_testing',
                    [
                        'defaultContext' => $context,
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => false,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )
            ->getMock();

        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    $context,
                ]
            )
            ->getMock();

        $labeller = new Tripod\Mongo\Labeller();

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($mockTables));

        $mockTables->expects($this->never())
            ->method('update');

        $originalGraph = $mockTripod->describeResource($uri);
        $updatedGraph = $originalGraph->get_subject_subgraph($uri);
        $updatedGraph->add_literal_triple($uri, $labeller->qname_to_uri('dct:description'), 'Physics textbook');

        $mockTripod->saveChanges($originalGraph, $updatedGraph);

        // The table row should still be there, even if the tablespec no longer exists
        $this->assertGreaterThan(0, $collection->count(['_id.type' => 't_resource', 'value._impactIndex' => [_ID_RESOURCE => $uri, _ID_CONTEXT => $context]]));
    }

    public function testCountTables()
    {
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with(['_id.type' => 't_source_count'])
            ->will($this->returnValue(50));

        $this->assertEquals(50, $tables->count('t_source_count'));
    }

    public function testCountTablesWithFilters()
    {
        $filters = ['_cts' => ['$lte' => new MongoDB\BSON\UTCDateTime(null)]];
        $query = array_merge(['_id.type' => 't_source_count'], $filters);
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with($query)
            ->will($this->returnValue(37));

        $this->assertEquals(37, $tables->count('t_source_count', $filters));
    }

    public function testDeleteTableRowsByTableId()
    {
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
            ->will($this->returnValue(2));

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with(['_id.type' => 't_source_count'])
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(2, $tables->deleteTableRowsByTableId('t_source_count'));
    }

    public function testDeleteTableRowsByTableIdWithTimestamp()
    {
        $timestamp = new MongoDB\BSON\UTCDateTime(null);

        $query = [
            '_id.type' => 't_source_count',
            '$or' => [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ],
        ];
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
            ->will($this->returnValue(11));

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with($query)
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(11, $tables->deleteTableRowsByTableId('t_source_count', $timestamp));
    }

    public function testTablesDocuments()
    {
        $dbDoc = [
            '_id' => [
                'r' => 'http://talis.com/modules/xmen-004',
                'c' => 'tenantContexts:DefaultGraph',
                'type' => 't_report_hierarchy',
            ],
            '_cts' => new MongoDB\BSON\UTCDateTime(1535454036),
            'value' => [
                '_id' => [
                    'r' => 'http://talis.com/modules/xmen-004',
                    'c' => 'tenantContexts:DefaultGraph',
                ],
                '_impactIndex' => [
                    ['r' => 'http://talis.com/modules/xmen-004', 'c' => 'tenantContexts:DefaultGraph'],
                    ['r' => 'http://talis.com/schools/xmen-001', 'c' => 'tenantContexts:DefaultGraph'],
                ],
                'code' => 'XMEN-004',
                'nodeUrl' => 'http://talis.com/modules/xmen-004',
                'name' => 'Psychology: Living with The Voices',
                'description' => 'Professor Deadpool will attempt to give you the ability to embrace your mental disorder and use it to your advantage. Whether you suffer from Schizophrenia, Dissociative Identity Disorder or plain old Comic Awareness, Wade Wilson has probably suffered through it himself. And while Deadpool can\'t solve your problem, he can teach you how to make it one of your most marketable qualities.',
                'parentCode' => 'XMEN-001',
                'parentNodeUrl' => 'http://talis.com/schools/xmen-001',
                'listCount' => 0,
                'type' => 'Module',
                'hasLinkedLists' => 'false',
            ],
        ];
        $doc = new Tripod\Mongo\Documents\Tables();
        $this->assertInstanceOf(MongoDB\Model\BSONDocument::class, $doc);
        $this->assertEquals([], $doc->getArrayCopy());
        $doc = new Tripod\Mongo\Documents\Tables($dbDoc);
        $this->assertEquals('XMEN-004', $doc['code']);
        $this->assertEquals('http://talis.com/modules/xmen-004', $doc['_id']['r']);
        $this->assertArrayNotHasKey('_cts', $doc);
        $this->assertArrayNotHasKey('_impactIndex', $doc);
        $this->assertArrayNotHasKey('type', $doc['_id']);
    }

    public function testGetTableRowsNoCount()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $tableRows = $this->tripodTables->getTableRows('t_resource', [], [], 0, 1, ['includeCount' => false]);

        $this->assertCount(1, $tableRows['results']);
        $this->assertEquals(-1, $tableRows['head']['count']);
    }

    public function testGetTableRowsReturnCursor()
    {
        $this->tripodTables->generateTableRows('t_resource');

        $tableRows = $this->tripodTables->getTableRows('t_resource', [], [], 0, 1, ['returnCursor' => true]);

        $this->assertInstanceOf(MongoDB\Driver\Cursor::class, $tableRows['results']);
        $count = 0;
        foreach ($tableRows['results'] as $result) {
            $this->assertInstanceOf(Tripod\Mongo\Documents\Tables::class, $result);
            $count++;
        }
        $this->assertEquals(1, $count);
        $this->assertGreaterThan(1, $tableRows['head']['count']);
    }
}
