<?php

/**
 * GedCache
 *
 * Adds a sqlite backed cache to php-gedcom. 
 *
 * The cache is automatically cleared and updated if the gedcom file is newer than the cache file.
 *
 * Note: The web server must be able to write to the sqlite cache file!
 *
 * Caches 0-level GEDCOM objects 
 *
 * Usage: Wherever you would've had 
 *      $parser = new PhpGedcom\Parser();
 *      $gedcom = $parser->parse('path/to/myfamily.ged');
 *
 * use this instead:
 *      $parser = new PhpGedcom\GedCache('path/to/my/sqlite_cache.sqlite');
 *      $gedcom = $parser->parse('path/to/myfamily.ged');
 *
 * Everything else should be transparent and magical.
 * 
 * Additional functionality beyond what's in PhpGedcom\Parser:
 */
namespace PhpGedcom;

class GedCache extends Parser {

    /**
     * @class PhpGedcom\GedCache
     *
     * Implement some of the methods in PhpGedCom\Parser methods to provide sqlite based caching of the GEDCOM file
     */

    var $cacheFile;
    var $cache = FALSE;
    var $gedcom;

    /**
     * @param $cacheFile (required) A path to the sqlite database
     */
    function __construct(\PhpGedCom\Gedcom $gedcom = null, $cacheFile = null){
        if(
        $this->cacheFile = $cacheFile;
    }

    /**
     * @brief Connect to a cache file and make sure it's filled
     *
     * @return TRUE on success, FALSE on failure
     */
    function connect($gedcom){
        $clear = (file_exists($this->cacheFile) && @filemtime($this->cacheFile) < @filemtime($gedcom));
        $create = (!file_exists($this->cacheFile) && @touch($this->cacheFile));
        $zero = (@filesize($this->cacheFile) === 0);

        if(!($this->cache = @$this->_connect())){
            return FALSE;
        }

        if(!$this->cache->beginTransaction()){
            return FALSE;
        }

        if($create || $zero){
            if(!$this->createCache()){
                return FALSE;
            }
        }

        // Preparing has to happen after the table is created!
        $this->insertCache = $this->cache->prepare("INSERT INTO cache (id,zeroobj,type,data) VALUES (:id,:zeroobj,:type,:data)");
        $this->defaultGet = $this->cache->prepare("SELECT data FROM cache WHERE zeroobj IS NULL AND type = :type");

        if($clear){
            if(!$this->clearCache()){
                return FALSE;
            }
        }

        if($create || $clear || $zero){
            if(!$this->fillCache($gedcom)){
                return FALSE;
            }
        }

        if(!$this->cache->commit()){
            return FALSE;
        }

        return TRUE;
    }

    /**
     * @brief Get a connection to the sqlite database
     *
     * @return A PDO connection or FALSE on failure
     */
    private function _connect(){
        try {
            $cache = @new \PDO('sqlite:' . $this->cacheFile);
            return $cache;
        } catch (\Exception $e){
            $this->catch = FALSE;
            error_log($e->getMessage());
            return FALSE;
        }
    }

    /**
     * @brief Create the cache database structure if it exists (doesn't fill it)
     *
     * @return TRUE on success, FALSE on failure
     */
    public function createCache(){
        if($this->cache){
            $workedSoFar = $this->cache->exec("CREATE TABLE IF NOT EXISTS cache (
                id VARCHAR PRIMARY KEY NOT NULL UNIQUE,
                zeroobj VARCHAR,
                type VARCHAR(4),
            data BLOB  
        )");

            if($workedSoFar === FALSE){
                return FALSE;
            }

            $workedSoFar = $this->cache->exec("CREATE INDEX IF NOT EXISTS zeros ON cache (zeroobj)");

            if(!$workedSoFar === FALSE){
                return FALSE;
            }

            $workedSoFar = $this->cache->exec("CREATE INDEX IF NOT EXISTS types ON cache (type)");

            if(!$workedSoFar === FALSE){
                return FALSE;
            }

            return TRUE;
        }
        return FALSE;
    }

    /**
     * @brief Clear the cache table
     *
     * @return TRUE on success, FALSE on failure
     */
    function clearCache(){
        if($this->cache){
            return $this->cache->exec("DELETE FROM cache");
        }
        return FALSE;
    }

     /**
      * @brief Fill the cache with the contents from the GEDCOM file
      *
      * @return TRUE on success, FALSE on failure
      */
     function fillCache($gedcom){
         $parser = new Parser();
         $gedcom = $parser->parse($gedcom);
 
         if($head = $gedcom->getHead()){
             if(!$this->cacheThis('HEAD',NULL,'HEAD',$head)){
                 return FALSE;
             }
         }
 
         if($subns = $gedcom->getSubn()){
             foreach($subns as $subn){
                 if(!$this->cacheThis(NULL,NULL,'SUBN',$subn)){
                     return FALSE;
                 }
             }
         }
 
         if($subms = $gedcom->getsubm()){
             foreach($subms as $subm){
                 if(!$this->cacheThis(NULL,NULL,'SUBM',$subm)){
                     return FALSE;
                 }
             }
         }
 
         if($sours = $gedcom->getSour()){
             foreach($sours as $sour){
                 if(!$this->cacheThis($sour->getSour(),NULL,'SOUR',$sour)){
                     return FALSE;
                 }
             }
         }
 
         if($indis = $gedcom->getindi()){
             foreach($indis as $indi){
                 if(!$this->cacheThis($indi->getId(),NULL,'INDI',$indi)){
                     return FALSE;
                 }
             }
         }
 
         if($fams = $gedcom->getfam()){
             foreach($fams as $fam){
                 if(!$this->cacheThis($fam->getId(),NULL,'FAM',$fam)){
                     return FALSE;
                 }
             }
         }
 
         if($notes = $gedcom->getnote()){
             foreach($notes as $note){
                 if(!$this->cacheThis($note->getId(),NULL,'NOTE',$note)){
                     return FALSE;
                 }
             }
         }
 
         if($repos = $gedcom->getrepo()){
             foreach($repos as $repo){
                 if(!$this->cacheThis($repo->getRepo(),NULL,'REPO',$repo)){
                     return FALSE;
                 }
             }
         }
 
         if($objes = $gedcom->getobje()){
             foreach($objes as $obje){
                 if(!$this->cacheThis($obje->getId(),NULL,'OBJE',$obje)){
                     return FALSE;
                 }
             }
         }
 
         return TRUE;
     }


    /**
     * @brief Implement the parse method from PhpGedcom\Parser
     *
     * @return Something 
     */
    function parse($gedcom){
        if(!$this->connect($gedcom)){
            return parent::parse($gedcom);
        }
        // Do low-memory parsing here
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
    function cacheThis($id = NULL,$zeroobj = NULL,$type = NULL,$data){
        // As long as we're still saving stuff, let's keep going!
        @set_time_limit(30);

        try {
            if(is_null($id)){
                $id = uniqid();
            }

            $this->insertCache->closeCursor();
            $this->insertCache->bindParam(":id",$id);
            $this->insertCache->bindParam(":zeroobj",$zeroobj);
            $this->insertCache->bindParam(":type",$type);
            $serialized = serialize($data);
            $this->insertCache->bindParam(":data",$serialized);
            return $this->insertCache->execute();
        }
        catch (\Exception $e){
            console_log($e->getMessage());
            return FALSE;
        }
    }

    /**
     * @brief Get the HEAD object
     *
     * @note Special case because HEAD is not an array
     * 
     * @return void or the HEAD object
     */
    function getHead(){
        if($this->cache === FALSE){
            return $this->gedcom->getHead();
        }

        $ca = $this->getCachedArray('HEAD');
        return $ca->current();
    }

    /**
     * @brief Magic
     */
    function __call($func,$args = Array()){
        // No cache, pass it through
        if($this->cache === FALSE){
            parent::__call($func,$args);
        }

        if(strpos($func,'get') === 0){
            $type = strtoupper(str_replace('get','',$func));
            return $this->getCachedArray($type);
        }
    }

    /**
     * @brief Get a PhpGedcom\CachedArray object of a specified type
     *
     * @return PhpGedcom\CachedArray object or FALSE. If FALSE a message will be logged.
     */
    private function getCachedArray($type){
        try {
            return new CachedArray($this->_connect(),$type);
        } catch (Exception $e){
            console_log($e->getMessage());
            return FALSE;
        }
    }
}

class CachedArray implements \Iterator,\ArrayAccess {

    /**
     * @class PhpGedcom\CachedArray
     *
     * Provide an object which allows array-like access to the sqlite cache so 
     * it can be treated like the results of PhpGedcom\Parser::getIndi and the likes.
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
