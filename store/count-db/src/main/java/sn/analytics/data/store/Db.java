package sn.analytics.data.store;


import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/* store data in multiple DBs */

/**
 * DB instance a shard in storage
 */
public  class Db {
    private Logger logger = LogManager.getLogger(Db.class);
    RocksDB db;
    String dbPath;
    //ReentrantLock writeLock = new ReentrantLock();
    final int instanceId;
    final boolean isReadOnly;

    /** Init DB */
    public Db(final String dbDir, final int instanceId){
        this.instanceId = instanceId;
        this.dbPath = dbDir;
        this.isReadOnly = false;
        initDb();
    }
    /** Init with option to turn on readonly */
    public Db(final String dbDir, final int instanceId,final boolean isReadOnly){
        this.instanceId = instanceId;
        this.dbPath = dbDir;
        this.isReadOnly = false;
        initDb();
    }

    static final String recSep = "~";

    public synchronized void initDb(){

        logger.info("init {} instance  {}" , dbPath   , instanceId);



        File instanceDataDir = new File(dbPath+ "\\"+instanceId);


        if (!instanceDataDir.exists()){
            instanceDataDir.mkdir();
        }
        dbPath = instanceDataDir.getAbsolutePath();
            Options options = new Options()
                .setCreateIfMissing(true)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(true)
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setEnablePipelinedWrite(true);

        try {
            if (!isReadOnly) {
                db = RocksDB.open(options,
                        dbPath);
            }else{
                db = RocksDB.openReadOnly(options,
                        dbPath);
            }
        } catch (Exception e) {
            logger.error("Error in init of DB instance {} {}",instanceId, dbPath);
            e.printStackTrace();
        }
    }

    public void close(){


        logger.info("compact & close {}",instanceId);
        try {
            if (!isReadOnly){
                db.compactRange();
            }
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized void addRecord(final String k, final String v){

        try {
            addRecord(k.getBytes(StandardCharsets.UTF_8),v.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized void addRecord(final byte [] k, final byte[] v){

        try{
            db.put(k,v);
        }catch (Exception e){
            logger.error("error in adding record {} {}", String.valueOf(k),e);
         }
    }


    public byte [] fetchRecord(final byte[] k){
        try{
            return db.get(k);
        }catch (Exception e){
            logger.error("exception in getting record {},{}",String.valueOf(k),e);
        }
        return null;
    }

    public synchronized void updateRecord(final String k , final String v){
        try{
            updateRecord(k.getBytes(StandardCharsets.UTF_8),v.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            logger.error("error in updating record {} instance {} {}",k,instanceId,e);

        }
    }

    public synchronized void updateRecord(final byte [] k, final byte [] v){
        try{
            db.merge(k,v);
        }catch (Exception e){
            logger.error("error in updating record {} instance {} {}",k,instanceId,e);
        }
    }


    public byte [] getRecord(final String k){

        Preconditions.checkArgument(k!=null,"Key cannot be null");
        try{
            return getRecord(k.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            logger.error("error in fetching record {} instance {} {}",k,instanceId,e);
            e.printStackTrace();
        }

        return null;
    }

    public byte[] getRecord(final byte [] k){
        Preconditions.checkArgument(k!=null,"Key cannot be null");
        try{
            return db.get(k);

        }catch (Exception e){
          // e.printStackTrace();
            logger.error("error in fetching record {} instance {} {}",k,instanceId,e);

        }
        return null;
    }

    public Set<byte[]> fetchRecordsBulk(final String k){

        Preconditions.checkArgument(k!=null,"Key cannot be null");
        return fetchRecordsBulk(k.getBytes(StandardCharsets.UTF_8));
    }

    public Set<byte[]> fetchRecordsBulk(final byte [] k){

        RocksIterator iterator = db.newIterator();

        String kStr = new String(k);
        String [] kParts = kStr.split(recSep);
        Set<byte[]> byteArrColl = new HashSet<>();

        iterator.seek(k);

        try{
            do {

                if (!iterator.isValid()){
                    break;
                }
                try {
                    byte[] karr = iterator.key();
                    byte[] varr = iterator.value();
                    String key = new String(karr);
                    String[] keyParts = key.split(recSep);
                    if (!keyParts[0].equalsIgnoreCase(kParts[0])) {
                        break;
                    }
                    byteArrColl.add(varr);
                }catch (Exception e){
                    logger.error("error in iterating for key {} ",kStr,e);
                }
                iterator.next();
            } while (true);

            return byteArrColl;
        }catch (Exception e){
            logger.error("Exception in fetching records for {} ",new String(k),e);
        }
        return null;
    }

}
