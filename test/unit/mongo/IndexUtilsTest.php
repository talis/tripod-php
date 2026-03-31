<?php

declare(strict_types=1);

use MongoDB\Collection;
use MongoDB\Driver\Manager;
use PHPUnit\Framework\MockObject\MockObject;
use Tripod\Mongo\IndexUtils;

class IndexUtilsTest extends MongoTripodTestBase
{
    public function testCBDCollectionIndexesAreCreated(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testCBDCollectionIndexesAreCreatedWithIndexOptions(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $indexOptions = ['unique' => true];

        $this->setConfigForCBDIndexes($config, $indexOptions);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true, $indexOptions);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testCBDCollectionIndexesAreCreatedInForeground(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, false);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testCBDCollectionIndexesAreCreatedInForegroundWithIndexOptions(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $indexOptions = ['unique' => true];

        $this->setConfigForCBDIndexes($config, $indexOptions);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, false, $indexOptions);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testCBDCollectionIndexesAreReindexed(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testCBDCollectionIndexesAreReindexedWithIndexOptions(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $indexOptions = ['unique' => true];

        $this->setConfigForCBDIndexes($config, $indexOptions);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true, $indexOptions);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreated(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreatedInForeground(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, false);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testViewIndexesAreReindexed(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreated(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreatedInForeground(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, false);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testTableIndexesAreReindexed(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreated(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreatedInForeground(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, false);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testSearchDocIndexesAreReindexed(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testIndexesAreDroppedOnlyOncePerCollectionWhenReindexed(): void
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDViewTableAndSearchDocIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    // HELPER METHODS

    /**
     * creates a mock IndexUtils object which will use the specified config.
     *
     * @param MockObject&TripodTestConfig $mockConfig mock config object
     *
     * @return IndexUtils&MockObject mocked IndexUtil object
     */
    private function createMockIndexUtils($mockConfig): MockObject
    {
        $mockIndexUtils = $this->getMockBuilder(IndexUtils::class)
            ->onlyMethods(['getConfig'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('getConfig')
            ->willReturn($mockConfig);

        return $mockIndexUtils;
    }

    /**
     * creates a mock mongo collection object.
     *
     * @return Collection&MockObject mock Collection object
     */
    private function createMockCollection(): MockObject
    {
        return $this->getMockBuilder(Collection::class)
            ->onlyMethods(['createIndex', 'dropIndexes'])
            ->setConstructorArgs([
                new Manager('mongodb://fake:27017'),
                'tripod_php_testing',
                'CBD_testing',
            ])
            ->getMock();
    }

    /**
     * creates a mock config object.
     *
     * @return MockObject&TripodTestConfig mock Config object
     */
    private function createMockConfig(): MockObject
    {
        return $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods([
                'getCollectionForCBD',
                'getCollectionForView',
                'getCollectionForTable',
                'getCollectionForSearchDocument',
            ])
            ->getMock();
    }

    /**
     * @param int                         $callCount      number of times Config->getCollectionForCBD should be called
     * @param MockObject&TripodTestConfig $mockConfig     mock Config object
     * @param Collection&MockObject       $mockCollection mock Collection object
     */
    private function getCollectionForCBDShouldBeCalled_n_Times(int $callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', 'CBD_testing')
            ->willReturn($mockCollection);
    }

    /**
     * @param int                         $callCount      number of times Config->getCollectionForView should be called
     * @param MockObject&TripodTestConfig $mockConfig     mock Config object
     * @param Collection&MockObject       $mockCollection mock Collection object
     */
    private function getCollectionForViewShouldBeCalled_n_Times(int $callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForView')
            ->with('tripod_php_testing', 'v_testview')
            ->willReturn($mockCollection);
    }

    /**
     * @param int                         $callCount      number of times Config->getCollectionForTable should be called
     * @param MockObject&TripodTestConfig $mockConfig     mock Config object
     * @param Collection&MockObject       $mockCollection mock Collection object
     */
    private function getCollectionForTableShouldBeCalled_n_Times(int $callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForTable')
            ->with('tripod_php_testing', 't_testtable')
            ->willReturn($mockCollection);
    }

    /**
     * @param int                         $callCount      number of times Config->getCollectionForSearchDocument should be called
     * @param MockObject&TripodTestConfig $mockConfig     mock Config object
     * @param Collection&MockObject       $mockCollection mock Collection object
     */
    private function getCollectionForSearchDocShouldBeCalled_n_Times(int $callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForSearchDocument')
            ->with('tripod_php_testing', 'i_search_something')
            ->willReturn($mockCollection);
    }

    /**
     * @param Collection&MockObject $mockCollection mock Collection object
     */
    private function dropIndexesShouldNeverBeCalled($mockCollection)
    {
        $mockCollection->expects($this->never())
            ->method('dropIndexes');
    }

    /**
     * @param Collection&MockObject $mockCollection mock Collection object
     */
    private function dropIndexesShouldBeCalled($mockCollection)
    {
        $mockCollection->expects($this->once())
            ->method('dropIndexes');
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function getCollectionForViewShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForView');
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function getCollectionForTableShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForTable');
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function getCollectionForSearchDocumentShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForSearchDocument');
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function getCollectionForCBDShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForCBD');
    }

    /**
     * Expectations one custom and three internal tripod indexes should be
     * created.
     *
     * createIndex is called 4 times, each time with a different set of params
     * a) one custom index is created based on the collection specification
     * b) three internal indexes are always created
     *
     * @param Collection&MockObject $mockCollection mock Collection object
     * @param bool                  $background     create indexes in the background
     * @param array<string, bool>   $indexOptions
     */
    private function oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($mockCollection, $background = true, array $indexOptions = [])
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the collection specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                [['rdf:type.u' => 1], array_merge(['name' => 'rdf_type', 'background' => $background], $indexOptions)],
                [[_ID_KEY => 1, _LOCKED_FOR_TRANS => 1], ['name' => '_lockedForTransIdx', 'background' => $background]],
                [[_ID_KEY => 1, _UPDATED_TS => 1], ['name' => '_updatedTsIdx', 'background' => $background]],
                [[_ID_KEY => 1, _CREATED_TS => 1], ['name' => '_createdTsIdx', 'background' => $background]]
            );
    }

    /**
     * @param Collection&MockObject $mockCollection mock Collection object
     * @param bool                  $background     create indexes in the background
     */
    private function oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(5))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1, _ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [['value.' . _IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]],
                [['rdf:type.u' => 1, '_cts' => 1], ['background' => $background]]
            );
    }

    /**
     * @param Collection&MockObject $mockCollection mock Collection object
     * @param bool                  $background     create indexes in the background
     */
    private function oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(5))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1, _ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [['value.' . _IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]],
                [['rdf:type.u' => 1], ['background' => $background]]
            );
    }

    /**
     * @param Collection&MockObject $mockCollection mock Collection object
     * @param bool                  $background     create indexes in the background
     */
    private function threeInternalTripodSearchDocIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 3 times, each time with a different set of
        // params that we know.
        // for search docs only internal indexes are created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]]
            );
    }

    /**
     * Returns tripod config to use on with the IndexUtils object
     * This is a minimal config used to assert what should happen when ensuring
     * indexes for a CBD collection.
     *
     * @param MockObject&TripodTestConfig $mockConfig   mock Config object
     * @param array<string, bool>         $indexOptions
     */
    private function setConfigForCBDIndexes($mockConfig, array $indexOptions = [])
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';

        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        if ($indexOptions === []) {
            $config['stores']['tripod_php_testing']['pods']['CBD_testing']['indexes'] = [
                'rdf_type' => [
                    'rdf:type.u' => 1,
                ],
            ];
        } else {
            $config['stores']['tripod_php_testing']['pods']['CBD_testing']['indexes'] = [
                'rdf_type' => [
                    [
                        'rdf:type.u' => 1,
                    ],
                    $indexOptions,
                ],
            ];
        }

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function setConfigForViewIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'view_specifications' => [
                    [
                        '_id' => 'v_testview',
                        'ensureIndexes' => [
                            ['rdf:type.u' => 1, '_cts' => 1],
                        ],
                        'from' => 'CBD_testing',
                        'type' => 'temp:TestType',
                        'joins' => [
                            'dct:partOf' => [],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function setConfigForTableIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'table_specifications' => [
                    [
                        '_id' => 't_testtable',
                        'ensureIndexes' => [
                            ['rdf:type.u' => 1],
                        ],
                        'from' => 'CBD_testing',
                        'type' => 'temp:TestType',
                        'fields' => [
                            [
                                'fieldName' => 'fieldA',
                                'predicates' => ['spec:note'],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function setConfigForSearchDocIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'search_config' => [
                    'search_provider' => 'MongoSearchProvider',
                    'search_specifications' => [
                        [
                            '_id' => 'i_search_something',
                            'type' => 'temp:TestType',
                            'from' => 'CBD_testing',
                            'filter' => [
                                'from' => 'CBD_testing',
                                'condition' => [
                                    'spec:name' => [
                                        '$exists' => true,
                                    ],
                                ],
                            ],
                            'fields' => [
                                [
                                    'fieldName' => 'result.title',
                                    'predicates' => ['spec:note'],
                                    'limit' => 1,
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&TripodTestConfig $mockConfig mock Config object
     */
    private function setConfigForCBDViewTableAndSearchDocIndexes($mockConfig)
    {
        $mockConfig->loadConfig([
            'defaultContext' => 'http://talisaspire.com/',
            'data_sources' => [
                'tlog' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://tloghost',
                ],
                'mongo' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://mongo',
                ],
            ],
            'stores' => [
                'tripod_php_testing' => [
                    'type' => 'mongo',
                    'data_source' => 'mongo',
                    'pods' => [
                        'CBD_testing' => [
                            'indexes' => [
                                'rdf_type' => ['rdf:type.u' => 1],
                            ],
                        ],
                    ],
                    'view_specifications' => [
                        [
                            '_id' => 'v_testview',
                            'ensureIndexes' => [
                                ['rdf:type.u' => 1, '_cts' => 1],
                            ],
                            'from' => 'CBD_testing',
                            'type' => 'temp:TestType',
                            'joins' => [
                                'dct:partOf' => [],
                            ],
                        ],
                    ],
                    'table_specifications' => [
                        [
                            '_id' => 't_testtable',
                            'ensureIndexes' => [
                                ['rdf:type.u' => 1],
                            ],
                            'from' => 'CBD_testing',
                            'type' => 'temp:TestType',
                            'fields' => [
                                [
                                    'fieldName' => 'fieldA',
                                    'predicates' => ['spec:note'],
                                ],
                            ],
                        ],
                    ],
                    'search_config' => [
                        'search_provider' => 'MongoSearchProvider',
                        'search_specifications' => [
                            [
                                '_id' => 'i_search_something',
                                'type' => 'temp:TestType',
                                'from' => 'CBD_testing',
                                'filter' => [
                                    'from' => 'CBD_testing',
                                    'condition' => [
                                        'spec:name' => ['$exists' => true],
                                    ],
                                ],
                                'fields' => [
                                    [
                                        'fieldName' => 'result.title',
                                        'predicates' => ['spec:note'],
                                        'limit' => 1,
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'transaction_log' => [
                'database' => 'transactions',
                'collection' => 'transaction_log',
                'data_source' => 'tlog',
            ],
        ]);
    }
}
