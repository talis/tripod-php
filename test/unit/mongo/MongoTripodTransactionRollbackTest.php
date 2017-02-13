<?php
require_once 'MongoTripodTestBase.php';
/** @noinspection PhpIncludeInspection */
require_once 'src/mongo/Driver.class.php';

/**
 * This test suite was added to specifically verify behaviour of code
 * during Driver->storeChanges.
 * namely that documents are locked, transactions created, documents unlocked etc.
 *
 * Class MongoTripodTransactionRollbackTest
 */
class MongoTripodTransactionRollbackTest extends MongoTripodTestBase
{
    /**
     * @var \Tripod\Mongo\Driver
     */
    protected $tripod = null;
    /**
     * @var \Tripod\Mongo\TransactionLog
     */
    protected $tripodTransactionLog = null;

    /**
     * @var \Tripod\Mongo\Labeller
     */
    protected $labeller = null;

    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->labeller = new \Tripod\Mongo\Labeller();

        // Stub out 'addToElastic' search to prevent writes into Elastic Search happening by default.
        $tripod = $this->getMock('\Tripod\Mongo\Driver', array('addToSearchIndexQueue'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripod->expects($this->any())->method('addToSearchIndexQueue');

        /** @var $tripod \Tripod\Mongo\Driver */
        \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing')->drop();

        // Lock collection no longer available from Driver, so drop it manually
        \Tripod\Mongo\Config::getInstance()->getCollectionForLocks('tripod_php_testing')->drop();

        $tripod->setTransactionLog($this->tripodTransactionLog);

        $this->tripod = $tripod;
    }

    public function testTransactionRollbackDuringLockAllDocuments()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000)
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000)
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g1);
        $nG->add_graph($g2);
        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('generateTransactionId','lockSingleDocument'), array($mockTripod));

        $mockTripodUpdate->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripodUpdate->expects($this->exactly(2*20)) //20 retries for 2 subjects
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCauseFailureCallback')));

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));

        /** @var $mockTripod \Tripod\Mongo\Driver */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);


        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('Exception should have been thrown');
        }
        catch (\Tripod\Exceptions\Exception $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $mockTripodUpdate->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("Did not obtain locks on documents", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);

    }

    public function testTransactionRollbackDuringLockAllDocumentsWithEmptyOriginalCBDS()
    {
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';

        $oG = new \Tripod\Mongo\MongoGraph();
        $nG = new \Tripod\Mongo\MongoGraph();
        // save two completely new entities
        $nG->add_resource_triple($subjectOne, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Resource"));
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title two");
        $nG->add_resource_triple($subjectTwo, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Book"));
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title four");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('generateTransactionId','lockSingleDocument'), array($mockTripod));

        $mockTripodUpdate->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripodUpdate->expects($this->exactly(2*20)) //20 retries for 2 subjects
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCauseFailureCallback')));

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));

        /** @var $mockTripod \Tripod\Mongo\Driver */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('Exception should have been thrown');
        }
        catch (\Tripod\Exceptions\Exception $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->is_empty());

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $mockTripodUpdate->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("Did not obtain locks on documents", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);
    }


    public function testTransactionRollbackDuringCreateTransaction()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000)
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*100),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate(strtotime("2013-03-21 00:00:00")*1000)
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g1);
        $nG->add_graph($g2);
        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        // some values we want explicitly returned from mocks
        $mockExpectedException = new \Tripod\Exceptions\Exception('Error creating new transaction');
        $mockTransactionId = 'transaction_1';

        $mockTransactionLog = $this->getMock('\Tripod\Mongo\TransactionLog', array('createNewTransaction', 'cancelTransaction','failTransaction'), array(),'',false, false);
        $mockTransactionLog->expects($this->once())
            ->method('createNewTransaction')
            ->will($this->throwException($mockExpectedException));
        $mockTransactionLog->expects(($this->once()))
            ->method('cancelTransaction')
            ->with($this->equalTo($mockTransactionId), $this->equalTo($mockExpectedException));
        $mockTransactionLog->expects(($this->once()))
            ->method('failTransaction')
            ->with($this->equalTo($mockTransactionId));

        $mockTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('generateTransactionId','lockSingleDocument', 'getTransactionLog'), array($mockTripod));

        $mockTripodUpdate->expects($this->once())
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripodUpdate->expects($this->exactly(2))
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCallback')));
        $mockTripodUpdate->expects($this->exactly(3))
            ->method('getTransactionLog')
            ->will($this->returnValue($mockTransactionLog));
        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));

        try
        {
            /* @var $mockTripod \Tripod\Mongo\Driver */
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('Exception should have been thrown');
        }
        catch (\Tripod\Exceptions\Exception $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        // make sure the documents are not polluted with locks
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
    }

    public function testTransactionRollbackDuringApplyChanges()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate()
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> \Tripod\Mongo\DateUtil::getMongoDate(),
            '_uts'=> \Tripod\Mongo\DateUtil::getMongoDate()
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);

        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'),
            array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripodUpdate = $this->getMock('\Tripod\Mongo\Updates',
            array('generateTransactionId','lockSingleDocument','applyChangeSet'), array($mockTripod));
        $mockTripodUpdate->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripodUpdate->expects($this->exactly(2))
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCallback')));
        $mockTripodUpdate->expects($this->once())->method('applyChangeSet')->will($this->throwException(new Exception("Exception throw by mock test during applychangeset")));
        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));

        /** @var $mockTripod \Tripod\Mongo\Driver */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('Exception should have been thrown');
        }
        catch (\Tripod\Exceptions\Exception $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $mockTripodUpdate->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("Exception throw by mock test during applychangeset", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);

    }


    /* HELPER FUNCTIONS BELOW HERE */

    /**
     * This helper function is a callback that assumes that the document being changed does not already exist in the db
     * Depending on the values passed it either mimics the behaviour of the real lockSingleDocument method on Driver, or it
     * returns an empty array. Have to do this because I want to mock the behavour of lockSingleDocument so I can throw an error for one subject
     * but allow it go through normally for another which you cant do with a mock, hence this hack!
     * @param $s
     * @param $transactionId
     * @param $context
     * @return array
     */
    public function lockSingleDocumentCauseFailureCallback($s, $transactionId, $context)
    {
        if($s=='http://example.com/resources/1')
        {
            return $this->lockSingleDocumentCallback($s, $transactionId, $context);
        } else {
            return array();
        }
    }

    /**
     * This is a private method that performs exactly the same operation as Driver::lockSingleDocument, the reason this is duplicated here
     * is so that we can simulate the correct locking of documents as part of mocking a workflow that will lock a document correctly but not another
     * @param $s
     * @param $transaction_id
     * @param $contextAlias
     * @return array
     */
    public function lockSingleDocumentCallback($s, $transaction_id, $contextAlias)
    {
        $lCollection = \Tripod\Mongo\Config::getInstance()->getCollectionForLocks($this->tripod->getStoreName());
        $countEntriesInLocksCollection = $lCollection->count(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));

        if($countEntriesInLocksCollection > 0) //Subject is already locked
            return false;
        else{
            try{ //Add a entry to locks collection for this subject, will throws exception if an entry already there
                $result = $lCollection->insertOne(
                    array(
                        '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias),
                        _LOCKED_FOR_TRANS => $transaction_id,
                        _LOCKED_FOR_TRANS_TS => \Tripod\Mongo\DateUtil::getMongoDate()
                    ),
                    array("w" => 1)
                );

                if(!$result->isAcknowledged()){
                    throw new Exception("Failed to lock document with error message- " . $this->getLastDBError());
                }
            }
            catch(Exception $e) { //Subject is already locked or unable to lock
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'Driver::lockSingleDocument - failed with exception',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'exception-message' => $e->getMessage()
                    )
                );
                return false;
            }

            //Let's get original document for processing.
            $document  = $this->getTripodCollection($this->tripod)->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
            if(empty($document)){ //if document is not there, create it
                try{
                    $result = $this->getTripodCollection($this->tripod)->insertOne(
                        array(
                            '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)
                        ),
                        array("w" => 1)
                    );

                    if(!$result->isAcknowledged()){
                        throw new Exception("Failed to create new document with error message- " . $this->getLastDBError());
                    }
                    $document  = $this->getTripodCollection($this->tripod)->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
                }
                catch(\Exception $e){
                    $this->errorLog(MONGO_LOCK,
                        array(
                            'description'=>'Driver::lockSingleDocument - failed when creating new document',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'exception-message' => $e->getMessage()
                        )
                    );
                    return false;
                }
            }
            return $document;
        }
    }

    private function getLastDBError($db = null) {
        if (is_null($db)) {
            $db = $this->config->getDatabase(
                $this->storeName,
                $this->config->getDataSourceForPod($this->storeName, $this->podName),
                $this->readPreference
            );
        }
        return $db->command([
            'getLastError' =>  1
        ])->toArray()[0];
    }
}

