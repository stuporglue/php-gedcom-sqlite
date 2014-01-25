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
 * Class ParserSqlite
 * @package PhpGedcom
 */
class ParserSqlite extends \PhpGedcom\Parser
{
    var $_cache;
    protected $_cacheFile;
    protected $_cacheEnabled = FALSE;
    protected $_cacheFull = FALSE;


    /**
     * @brief ParserSqlite should work just like Parser, except that it will try to have an sqlite backing 
     */
    public function __construct(\PhpGedcom\Gedcom $gedcom = null,$cacheFile = null)
    {
        $this->_gedcom = $gedcom;
        $this->_cacheFile = $cacheFile;
    }

    /**
     * @brief Parse a GEDCOM file. Wrap the parsing in a transaction if we're database backed
     */
    public function parse($fileName){

        if(is_null($this->_gedcom)){
            $gedcomCache = $this->getGedcomCache($fileName);
            if($gedcomCache){
                // And instead of being Gedcoms, we were GedcomSqlites
                $this->_gedcom = $gedcomCache;
            }else{
                $this->_gedcom = new \PhpGedcom\Gedcom();
                return parent::parse($fileName);
            }
        }

        if(!$this->_cacheEnabled){
            return parent::parse($fileName);
        }

        if($this->_cacheFull){
            return $this->getGedcom();
        }

        $this->getGedcom()->_cache->beginTransaction();  

        $this->_file = fopen($fileName, 'r'); #explode("\n", mb_convert_encoding($contents, 'UTF-8'));

        if (!$this->_file) {
            return null;
        }

        $this->forward();

        while (!$this->eof()) {
            $record = $this->getCurrentLineRecord();

            if ($record === false) {
                continue;
            }

            $depth = (int)$record[0];

            // We only process 0 level records here. Sub levels are processed
            // in methods for those data types (individuals, sources, etc)

            if ($depth == 0) {
                // Although not always an identifier (HEAD,TRLR):
                $identifier = $this->normalizeIdentifier($record[1]);

                if (trim($record[1]) == 'HEAD') {
                    $head = Parser\Head::parse($this);
                    $this->getGedcom()->cacheThis($head,'HEAD','HEAD');
                } else if (isset($record[2]) && trim($record[2]) == 'SUBN') {
                    $subn = Parser\Subn::parse($this);
                    $this->getGedcom()->cacheThis($subn,'SUBN','SUBN');
                } else if (isset($record[2]) && trim($record[2]) == 'SUBM') {
                    $subm = Parser\Subm::parse($this);
                    $this->getGedcom()->cacheThis($subm,$subm->getSubm(),'SUBM');
                } else if (isset($record[2]) && $record[2] == 'SOUR') {
                    $sour = Parser\Sour::parse($this);
                    $this->getGedcom()->cacheThis($sour,$sour->getSour(),'SOUR');
                } else if (isset($record[2]) && $record[2] == 'INDI') {
                    $indi = Parser\Indi::parse($this);
                    $this->getGedcom()->cacheThis($indi,$indi->getId(),'INDI');
                } else if (isset($record[2]) && $record[2] == 'FAM') {
                    $fam = Parser\Fam::parse($this);
                    $this->getGedcom()->cacheThis($fam,$fam->getId(),'FAM');
                } else if (isset($record[2]) && substr(trim($record[2]), 0, 4) == 'NOTE') {
                    $note = Parser\Note::parse($this);
                    $this->getGedcom()->cacheThis($note,$note->getId(),'NOTE');
                } else if (isset($record[2]) && $record[2] == 'REPO') {
                    $repo = Parser\Repo::parse($this);
                    $this->getGedcom()->cacheThis($repo,$repo->getRepo(),'REPO');
                } else if (isset($record[2]) && $record[2] == 'OBJE') {
                    $obje = Parser\Obje::parse($this);
                    $this->getGedcom()->cacheThis($obje,$obje->getId(),'OBJE');
                } else if (trim($record[1]) == 'TRLR') {
                    // EOF
                    break;
                } else {
                    $this->logUnhandledRecord(get_class() . ' @ ' . __LINE__);
                }
            } else {
                $this->logUnhandledRecord(get_class() . ' @ ' . __LINE__);
            }

            $this->forward();
        }

        $this->getGedcom()->_cache->commit();  

        return $this->getGedcom();
    }

    /**
     * @brief Connect to a cache file and make sure it's filled
     *
     * @param $fileName (optional) A GEDCOM file to check if cache is valid
     *
     * @return TRUE on success, FALSE on failure
     */
    private function getGedcomCache($fileName = NULL){

        if(is_null($this->_cacheFile)){
            return FALSE;
        }

        // There are three possible states we can handle for the cache
        // 1: No cache file
        // 2: Outdated or empty cache file
        // 3: Good cache file

        $clear = (file_exists($this->_cacheFile) && !is_null($fileName) && @filemtime($this->_cacheFile) < @filemtime($fileName));
        $create = (!file_exists($this->_cacheFile) && @touch($this->_cacheFile));
        $zero = (@filesize($this->_cacheFile) === 0);

        if(!($this->_cache = @$this->_connect())){
            return FALSE;
        }

        if(!$this->_cache->beginTransaction()){
            return FALSE;
        }

        if($create || $zero){
            if(!$this->createCache()){
                return FALSE;
            }
        }

        // Preparing has to happen after the table is created!
        $this->defaultGet = $this->_cache->prepare("SELECT data FROM cache WHERE zeroobj IS NULL AND type = :type");
        $this->cacheCount = $this->_cache->prepare("SELECT COUNT(*) AS count FROM cache");

        // Couldn't prepare statements!
        if(!$this->defaultGet || !$this->cacheCount){
            return FALSE;
        }

        if($clear){
            if(!$this->clearCache()){
                return FALSE;
            }
        }

        if($create || $clear || $zero){
            $this->_cacheFull = FALSE;
        }else{
            // Make sure we actually have records
            $this->cacheCount->execute();
            $row = $this->cacheCount->fetch(\PDO::FETCH_ASSOC);
            if($row['count'] == 0){
                $this->_cacheFull = FALSE;
            }else{
                $this->_cacheFull = TRUE;
            }
        }


        if(!$this->_cache->commit()){
            return FALSE;
        }

        $this->_cache = FALSE;

        $this->_cacheEnabled = TRUE;
        return new \PhpGedcom\GedcomSqlite($this->_cacheFile,($create || $clear || $zero));
    }

    /**
     * @brief Get a connection to the sqlite database
     *
     * @return A PDO connection or FALSE on failure
     */
    private function _connect(){
        try {
            $cache = @new \PDO('sqlite:' . $this->_cacheFile);
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
    private function createCache(){
        if($this->_cache){
            $workedSoFar = $this->_cache->exec("CREATE TABLE IF NOT EXISTS cache (
                id VARCHAR PRIMARY KEY NOT NULL UNIQUE,
                zeroobj VARCHAR,
                type VARCHAR(4),
            data BLOB  
        )");

            if($workedSoFar === FALSE){
                return FALSE;
            }

            $workedSoFar = $this->_cache->exec("CREATE INDEX IF NOT EXISTS zeros ON cache (zeroobj)");

            if(!$workedSoFar === FALSE){
                return FALSE;
            }

            $workedSoFar = $this->_cache->exec("CREATE INDEX IF NOT EXISTS types ON cache (type)");

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
    private function clearCache(){
        if($this->_cache){
            return $this->_cache->exec("DELETE FROM cache");
        }
        return FALSE;
    }
}
