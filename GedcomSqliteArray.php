<?php
/**
 * php-gedcom-sqlite
 *
 * php-gedcom-sqlite is a library for parsing, manipulating, importing and exporting
 * GEDCOM 5.5 files in PHP 5.3+.
 *
 * @author          Michael Moore <stuporglue@gmail.com>
 * @copyright       Copyright (c) 2013, Michael Moore
 * @package         php-gedcom-sqlite
 * @license         GPL-3.0
 * @link            https://github.com/stuporglue/php-gedcom-sqlite
 */

namespace PhpGedcom;

/**
 * Class GedcomSqliteArray
 * @package PhpGedcom
 */
class GedcomSqliteArray implements \Iterator,\ArrayAccess {

    /**
     * @class PhpGedcom\GedcomSqliteArray
     *
     * Provide an object which allows array-like access to the sqlite cache so 
     * it can be treated like the results of PhpGedcom\Parser::getIndi and the likes.
     *
     * Act like an array of objects as found inside PhpGedcom\Gedcom
     */

    // The database cursor
    var $cursor;

    // We want a non-buffered cursor so we use less memory. 
    // To make up for not being able to grab the current position
    // we hold one in a staging area sometimes
    var $nextVal;

    /**
     * @param $pdo (required) A PDO connection to the cache. 
     * @param $type (required) The type of CachedArray this object represents
     *
     * @note Each CachedArray gets its own PDO because concurrent prepared statement queries can cause problems 
     */
    function __construct($pdo,$type){
        $this->cursor = $pdo->prepare("SELECT * FROM cache WHERE type=:type");
        $this->cursor->bindParam(":type",$type);
        $this->cursor->execute();

        $this->lookup = $pdo->prepare("SELECT * FROM cache WHERE type=:type AND id=:id LIMIT 1");
        $this->lookup->bindParam(":type",$type);
    }

    // Implement the Iterator interface
    function current(){
        if($this->_getNext()){
            $data = unserialize($this->nextVal['data']);
            return $data;
        }
        return FALSE;
    }

    function key(){
        if($this->_getNext()){
            return $this->nextVal['id'];
        }
    }

    function next(){
        unset($this->nextVal);
        $this->_getNext();
    }

    function rewind(){
        $this->cursor->closeCursor();
        $this->cursor->execute();
    }

    function valid(){
        return $this->_getNext();
    }

    // Implement the ArrayAccess interface
    function offsetExists($offset){
        $this->lookup->bindParam(':id',$offset);
        $this->lookup->execute();
        $notFound = ($this->lookup->fetch() === FALSE);
        $this->lookup->closeCursor();
        return !$notFound;
    }

    function offsetGet($offset){
        $this->lookup->bindParam(':id',$offset);
        $this->lookup->execute();
        $res = $this->lookup->fetch(\PDO::FETCH_ASSOC);
        $this->lookup->closeCursor();
        $data = unserialize($res['data']);
        return $data;
    }

    function offsetSet($offset,$value){
        // do nothing
    }

    function offsetUnset($offset){
        // do nothing
    }

    /**
     * @brief Fetch a single value from the database cursor and hold it
     *
     * @return TRUE if we end holding a value, FALSE if not
     */
    private function _getNext(){
        if(isset($this->nextVal)){
            return TRUE;
        }

        $newCur = $this->cursor->fetch(\PDO::FETCH_ASSOC);
        if($newCur !== FALSE){
            $this->nextVal = $newCur;
            return TRUE;
        }
        return FALSE;
    }
}
