<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/Config.class.php';
require_once 'src/mongo/util/DateUtil.class.php';

class DateUtilTest extends MongoTripodTestBase
{
    public function testGetMongoDateWithNoParam()
    {
        $config = \Tripod\Mongo\Config::getInstance();
        $updatedAt = (new \Tripod\Mongo\DateUtil())->getMongoDate();

        $_id = array(
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/');
        $doc = array(
            '_id' => $_id,
            'dct:title' => array('l'=>'etag'),
            '_version' => 0,
            '_cts' => $updatedAt,
            '_uts' => $updatedAt
        );
        $config->getCollectionForCBD(
            'tripod_php_testing',
            'CBD_testing'
        )->insertOne($doc, array("w"=>1));

        $date = \Tripod\Mongo\DateUtil::getMongoDate();

        $this->assertInstanceOf('\MongoDB\BSON\UTCDateTime', $date);
        $this->assertEquals(13, strlen($date->__toString()));
    }
    public function testGetMongoDateWithParam()
    {
        $config = \Tripod\Mongo\Config::getInstance();
        $updatedAt = (new \Tripod\Mongo\DateUtil())->getMongoDate();

        $_id = array(
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/');
        $doc = array(
            '_id' => $_id,
            'dct:title' => array('l'=>'etag'),
            '_version' => 0,
            '_cts' => $updatedAt,
            '_uts' => $updatedAt
        );
        $config->getCollectionForCBD(
            'tripod_php_testing',
            'CBD_testing'
        )->insertOne($doc, array("w"=>1));

        $time = floor(microtime(true) * 1000);
        $date = \Tripod\Mongo\DateUtil::getMongoDate($time);

        $this->assertInstanceOf('\MongoDB\BSON\UTCDateTime', $date);
        $this->assertEquals(13, strlen($date->__toString()));
        $this->assertEquals($time, $date->__toString());
    }
}