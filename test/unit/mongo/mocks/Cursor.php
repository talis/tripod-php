<?php

namespace Tripod\Test\Mongo\Mocks;

if (PHP_VERSION_ID >= 80000) {
    require_once __DIR__ . '/CursorPhp80.php';
    class Cursor extends CursorPhp80 {}
} else {
    require_once __DIR__ . '/CursorPhp74.php';
    class Cursor extends CursorPhp74 {}
}
