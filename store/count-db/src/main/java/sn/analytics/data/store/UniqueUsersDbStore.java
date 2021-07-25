package sn.analytics.data.store;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.sangupta.murmur.Murmur3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Store unique users per day
 * Class Db.class has all RocksDB ops, this manages sharding & HLL ops
 * Data model:
 *              K=day V=HLL,
 *               Sharded by K
 */
public class UniqueUsersDbStore {

    private static final Logger logger = LogManager.getLogger(UniqueUsersDbStore.class);
    private static final UniqueUsersDbStore _instance = new UniqueUsersDbStore();
    private UniqueUsersDbStore(){}
    public static UniqueUsersDbStore getInstance(){return _instance;}
    private String dbPath="/tmp";
    private int maxInstances=8;
    private boolean isReadOnly=false;
    private Map<Integer,Db> dbShardColl;
    private static final int _seed=299_792_458;
    private static final int _hllBits = 16;


    static final String recSep = "~";

    /** init with base path & max instances*/
    public synchronized void init(final String dbPath, final int maxInstances){
        init(dbPath,maxInstances,false);
    }
    /** init read-only instance*/
    public synchronized void init(final String dbPath, final int maxInstances,final boolean readOnly){
        this.dbPath=dbPath;
        this.maxInstances=maxInstances;
        this.isReadOnly=readOnly;
        initDbShards();
    }



    public void initDbShards(){

        Map<Integer,Db> dbshards= new HashMap<>(maxInstances);

        for(int i =0;i < maxInstances;i++){
            try{
                Db db = new Db(dbPath,i,isReadOnly);
                dbshards.put(i,db);
            }catch (Exception e){
                logger.error("Error in initializing Db shards {}",i);
                throw new RuntimeException("Error in initializing DB shard {}");
            }
        }
        dbShardColl = ImmutableMap.copyOf(dbshards);
        logger.info("initialized db shards {} DB path {}", dbPath,maxInstances);
    }
    //shard based on hashing

    public int getInstanceSlot(final String str){

        return Hashing.consistentHash( Murmur3.hash_x64_128(str.getBytes(), str.getBytes().length, _seed)[0] >>> 1,maxInstances);
    }

    public int getInstanceSlot(final byte [] b){

        return Hashing.consistentHash( Murmur3.hash_x64_128(b, b.length, _seed)[0] >>> 1,maxInstances);
    }

    //DB ops

    public void addRecord(final String k, final String v) {


        int instanceSlot = getInstanceSlot(k);
        //find the db and insert into right DB
        Db db = dbShardColl.get(instanceSlot);
        if (db!=null){
            try {
                db.addRecord(k, v);
            }catch (Exception e){

                logger.error("Exception in adding record {} {}",k,e);
            }
        }
    }
    public void addRecord(final String k, final byte[] v) {


        int instanceSlot = getInstanceSlot(k);

        Db db = dbShardColl.get(instanceSlot);
        if (db!=null){
            try {
                db.addRecord(k.getBytes(StandardCharsets.UTF_8), v);
            }catch (Exception e){
                e.printStackTrace();
                logger.error("Exception in adding record {} ,{}",k,e);

            }
        }
    }

    public byte[] getRecord(final String k){
        int instanceSlot = getInstanceSlot(k);
        Db db = dbShardColl.get(instanceSlot);
        if (db!=null){
            try {
                return db.getRecord(k);
            }catch (Exception e){
                logger.error("Exception in fetching record {} ,{}",k,e);
                e.printStackTrace();
            }
        }
        return null;
    }

    public byte[] getRecord(final String prefix, final String k){
        int instanceSlot = getInstanceSlot(prefix);
        Db db = dbShardColl.get(instanceSlot);
        if (db!=null){
            try {
                return db.getRecord(k);
            }catch (Exception e){
                e.printStackTrace();

            }
        }

        return null;
    }

    public void addRecord(final String prefix, final String key , final byte [] v){
        int instanceSlot = getInstanceSlot(prefix);
        //find the db and insert into right DB
        Db db = dbShardColl.get(instanceSlot);
        if (db!=null) {
            db.addRecord(key.getBytes(StandardCharsets.UTF_8), v);
        }
    }

    public void close(){
        try {
            dbShardColl.values().forEach(Db::close);
        }catch (Exception e){
            logger.error("error in closing instances ",e);
        }
    }

    //HLL Ops
    /** Add users set day format yyyyMMdd */
    public void addUniqueUsers(final String day, Set<Long> usersSet){
        Preconditions.checkArgument(day!=null,"Day cannot be null");
        Preconditions.checkArgument(usersSet!=null,"users set cannot be null");
        //Preconditions.checkArgument(!usersSet.isEmpty(),"Users set empty");
        //make it HLL and persist based on key
        try {
            HyperLogLog hll = HyperLogLog.Builder.withLog2m(_hllBits).build();
            //offer would hash the data as required
            usersSet.forEach(hll::offer);
            if (logger.isDebugEnabled())
                logger.debug("insert hll {} set size {}, hll cardinality {} ",day,usersSet.size(),hll.cardinality());
            addRecord(day,hll.getBytes());

        }catch (Exception e){
            logger.error("error in insert record {} day",day,e);
        }
    }

    public Long getUniqueUserCount(final String day){
        Preconditions.checkArgument(day!=null,"Day cannot be null");
        try{
            byte [] userHll = getRecord(day);
            if (userHll!=null){
               HyperLogLog hll = HyperLogLog.Builder.build(userHll);
               if (hll!=null){
                   return hll.cardinality();
               }
            }
        }catch (Exception e){
            logger.error("error in fetching HLL records from store ",e);
        }
        return 0L;
    }


    public Long getUniqueUserCount(final String ... days){
        Preconditions.checkArgument(days!=null,"Day cannot be null");
        List<HyperLogLog> hllColl = new ArrayList<>();
        try{


            for(String day : days) {

                byte[] userHll = getRecord(day);
                if (userHll != null) {
                    HyperLogLog hll = HyperLogLog.Builder.build(userHll);
                    if (logger.isDebugEnabled()) {
                        logger.debug("fetched {} cardinality {} " ,day, hll.cardinality());
                    }
                    if (hll != null) {
                        hllColl.add(hll);
                    }

                }
            }
            if (!hllColl.isEmpty()) {
                HyperLogLog hllSrc = HyperLogLog.Builder.withLog2m(16).build();
                HyperLogLog[] hllArr = new HyperLogLog[hllColl.size()];
                hllColl.toArray(hllArr);
                ICardinality hllMerged = hllSrc.merge(hllArr);

                return hllMerged.cardinality();
            }
        }catch (Exception e){
            logger.error("error in fetching HLL / merging ",e);
        }
        return 0L;
    }


}
