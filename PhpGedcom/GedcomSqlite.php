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
 * Class GedcomSqlite
 * @package PhpGedcom
 */
class GedcomSqlite 
{
    protected $_needsFilling = TRUE;
    protected $_cacheFile;
    var $_cache;

    /**
     * @class PhpGedcom\GedcomSqlite
     * 
     * Handle caching and retreval of PhpGedcom objects into an SQLite database
     *
     * Act like a PhpGedcom\Gedcom object
     */

    function __construct($cacheFile,$needsFilling = TRUE){
        $this->_cacheFile = $cacheFile;
        $this->_needsFilling = $needsFilling;
        $this->_cache = $this->_connect();
        $this->insertCache = $this->_cache->prepare("INSERT INTO cache (id,zeroobj,type,data) VALUES (:id,:zeroobj,:type,:data)");
    }

    /**
     * @brief Get a connection to the sqlite database
     *
     * @return A PDO connection or FALSE on failure
     */
    private function _connect(){
        try {
            $cache = @new \PDO('sqlite:' . $this->_cacheFile);
            $cache->setAttribute(\PDO::ATTR_EMULATE_PREPARES,TRUE);

            if(!$cache){
                return FALSE;
            }

            return $cache;
        } catch (\Exception $e){
            error_log($e->getMessage());
            return FALSE;
        }
    }

    /**
     * @brief Cache a single record
     *
     * @param $id (optional) They key for the object being cached. If not provided a uuid will be generated
     * @param $zeroobj (optional) If the object is not a top-level object, list its 0-level parent object (for future use)
     * @param $type (semi-required) The type of object being cached. If not provided no methods here will help you look it up.
     * @param $data (required) The object to cache
     *
     * @return TRUE on success, FALSE on failure
     */
    public function cacheThis($data,$id = NULL,$type = NULL,$zeroobj = NULL){

        // As long as we're still saving stuff, let's keep going!
        try {
            if(is_null($id)){
                $id = uniqid();
            }

            $this->insertCache->bindParam(":id",$id);
            $this->insertCache->bindParam(":zeroobj",$zeroobj);
            $this->insertCache->bindParam(":type",$type);
            $serialized = serialize($data);
            $this->insertCache->bindParam(":data",$serialized);
            return $this->insertCache->execute();
        } catch (\Exception $e){
            console_log($e->getMessage());
            return FALSE;
        }
    }

    /**
     * @brief Get a PhpGedcom\CachedArray object of a specified type
     *
     * @return PhpGedcom\CachedArray object or FALSE. If FALSE a message will be logged.
     */
    private function getCachedArray($type){
        try {
            return new \PhpGedcom\GedcomSqliteArray($this->_connect(),$type);
        } catch (Exception $e){
            console_log($e->getMessage());
            return FALSE;
        }
    }

    function __call($func,$args){
        if(strpos($func,'add') === 0){
            return TRUE;
        }
        if(strpos($func,'set') === 0){
            return TRUE;
        }

        if(strpos($func,'get') === 0){
            $type = strtoupper(str_replace('get','',$func));

            $ca = $this->getCachedArray($type);

            // SUBN and HEAD aren't arrays, so just return the single object
            if($type == 'SUBN' || $type == 'HEAD'){
                return $ca->current();
            }

            return $ca;
        }
    }
}
