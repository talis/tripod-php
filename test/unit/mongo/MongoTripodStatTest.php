<?php

declare(strict_types=1);

use Tripod\Mongo\NoStat;
use Tripod\StatsD;
use Tripod\TripodStatFactory;

class MongoTripodStatTest extends MongoTripodTestBase
{
    public function testStatFactory(): void
    {
        $statConfig = $this->getStatsDConfig();

        /** @var StatsD */
        $stat = TripodStatFactory::create($statConfig);
        $this->assertInstanceOf(StatsD::class, $stat);
        $this->assertEquals('example.com', $stat->getHost());
        $this->assertEquals(1234, $stat->getPort());
        $this->assertSame('somePrefix', $stat->getPrefix());

        $noStat = TripodStatFactory::create();
        $this->assertInstanceOf(NoStat::class, $noStat);
    }

    public function testStatsDSettersAndGetters(): void
    {
        $stat = StatsD::createFromConfig($this->getStatsDConfig());

        $this->assertInstanceOf(StatsD::class, $stat);
        $this->assertEquals('example.com', $stat->getHost());
        $this->assertEquals(1234, $stat->getPort());
        $this->assertSame('somePrefix', $stat->getPrefix());

        $this->assertEquals($this->getStatsDConfig(), $stat->getConfig());

        $stat = new StatsD('foo.bar', 9876);
        $this->assertEquals('foo.bar', $stat->getHost());
        $this->assertEquals(9876, $stat->getPort());
        $this->assertSame('', $stat->getPrefix());
        $this->assertSame(['class' => StatsD::class, 'config' => ['host' => 'foo.bar', 'port' => 9876, 'prefix' => '']], $stat->getConfig());

        $stat->setHost('bar.baz');
        $this->assertEquals('bar.baz', $stat->getHost());

        $stat->setPort(4567);
        $this->assertEquals(4567, $stat->getPort());
        $stat->setPrefix('FOO_BAR');
        $this->assertSame('FOO_BAR', $stat->getPrefix());

        $this->assertSame(['class' => StatsD::class, 'config' => ['host' => 'bar.baz', 'port' => 4567, 'prefix' => 'FOO_BAR']], $stat->getConfig());
    }

    public function testStatsDIncrementNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [STAT_CLASS . '.FOO.BAR' => '1|c'],
                1
            );

        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPivotValueNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);

        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    STAT_CLASS . '.FOO.BAR' => '1|c',
                ],
                1
            );

        $stat->setPivotValue('wibble');

        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                ['somePrefix.' . STAT_CLASS . '.FOO.BAR' => '1|c'],
                1
            );

        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPivotValueAndPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    'somePrefix.' . STAT_CLASS . '.FOO.BAR' => '5|c',
                ],
                1
            );

        $stat->setPivotValue('wibble');
        $stat->increment('FOO.BAR', 5);
    }

    public function testStatsDTimerNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [STAT_CLASS . '.FOO.BAR' => ['1|c', '1234|ms']],
                1
            );

        $stat->timer('FOO.BAR', 1234);
    }

    public function testStatsDTimerWithPivotValueNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    STAT_CLASS . '.FOO.BAR' => ['1|c', '1234|ms'],
                ],
                1
            );

        $stat->setPivotValue('wibble');
        $stat->timer('FOO.BAR', 1234);
    }

    public function testStatsDTimerWithPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                ['somePrefix.' . STAT_CLASS . '.FOO.BAR' => ['1|c', '4567|ms']],
                1
            );

        $stat->timer('FOO.BAR', 4567);
    }

    public function testStatsDTimerWithPrefixAndPivotValue(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    'somePrefix.' . STAT_CLASS . '.FOO.BAR' => ['1|c', '4567|ms'],
                ],
                1
            );

        $stat->setPivotValue('wibble');
        $stat->timer('FOO.BAR', 4567);
    }

    public function testStatsDGaugeNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [STAT_CLASS . '.FOO.BAR' => 'xyz|g'],
                1
            );

        $stat->gauge('FOO.BAR', 'xyz');
    }

    public function testStatsDGaugeWithPivotValueNoPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    STAT_CLASS . '.FOO.BAR' => 'xyz|g',
                ],
                1
            );
        $stat->setPivotValue('wibble');

        $stat->gauge('FOO.BAR', 'xyz');
    }

    public function testStatsDGaugeWithPrefix(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                ['somePrefix.' . STAT_CLASS . '.FOO.BAR' => 'abc|g'],
                1
            );

        $stat->gauge('FOO.BAR', 'abc');
    }

    public function testStatsDGaugeWithPrefixAndPivotValue(): void
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                [
                    'somePrefix.' . STAT_CLASS . '.FOO.BAR' => 'abc|g',
                ],
                1
            );

        $stat->setPivotValue('wibble');
        $stat->gauge('FOO.BAR', 'abc');
    }

    public function testPrefixCannotStartWithDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid prefix supplied');

        new StatsD('foo.bar', 4567, '.some_prefix');
    }

    public function testPrefixCannotEndWithDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid prefix supplied');

        new StatsD('foo.bar', 4567, 'some_prefix.');
    }

    public function testPrefixCannotContainConsecutiveDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid prefix supplied');

        new StatsD('foo.bar', 4567, 'some..prefix');
    }

    public function testPivotValueCannotStartWithDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid pivot value supplied');

        $stat = new StatsD('foo.bar', 4567);
        $stat->setPivotValue('.someValue');
    }

    public function testPivotValueCannotEndWithDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid pivot value supplied');

        $stat = new StatsD('foo.bar', 4567);
        $stat->setPivotValue('someValue.');
    }

    public function testPivotValueCannotContainConsecutiveDot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid pivot value supplied');

        $stat = new StatsD('foo.bar', 4567);
        $stat->setPivotValue('some..value');
    }
}
