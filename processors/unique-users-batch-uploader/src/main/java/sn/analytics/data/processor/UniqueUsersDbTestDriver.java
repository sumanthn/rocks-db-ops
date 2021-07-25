package sn.analytics.data.processor;

import com.google.common.collect.Sets;
import sn.analytics.data.store.UniqueUsersDbStore;
import sn.analytics.data.store.UniqueUsersDbStoreV2;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UniqueUsersDbTestDriver {

    static void writePrefixTest(){
        UniqueUsersDbStoreV2.getInstance().init("users-dump-new",8);

        Set<Long> globalIds = new HashSet<>();


        Random rgen = new Random(4000);
        String [] days = new String[]{"20210301","20210302","20210304","20210305","20210303"};

        for(String day : days) {

            for (int p = 0 ;p < 12; p++) {
                Set<Long> ids =
                        Sets.newHashSet();
                for (int i = 0; i < 500; i++) {
                    //ids.add((long)i);
                    ids.add(rgen.nextLong());

                }
                UniqueUsersDbStoreV2.getInstance().addUniqueUsers(day, ids);

                globalIds.addAll(ids);

            }
        }
        System.out.println("global ids " + globalIds.size());
        UniqueUsersDbStoreV2.getInstance().close();
    }

    static void  writeTest(){
        UniqueUsersDbStore.getInstance().init("C:\\Users\\Sumanth\\rockdb-projs\\dump-new",8);

        Set<Long> globalIds = new HashSet<>();


        Random rgen = new Random(4000);
        String [] days = new String[]{"20210301","20210302","20210304","20210305","20210303"};

        for(String day : days) {
            Set<Long> ids =
                    Sets.newHashSet();
            for (int i = 0; i < 10000; i++) {

                //ids.add((long)i);
                ids.add(rgen.nextLong());

            }
            UniqueUsersDbStore.getInstance().addUniqueUsers(day,ids);
            globalIds.addAll(ids);

        }
        System.out.println("global ids " + globalIds.size());
        UniqueUsersDbStore.getInstance().close();

    }
    static void readTestPrefixed(){
        String [] days = new String[]{"20210301","20210302","20210304","20210305","20210303"};
        //String day = "20210304";
        UniqueUsersDbStoreV2.getInstance().init("user-dump-new",8,true);

        try{
            Long mergedCardinality = UniqueUsersDbStoreV2.getInstance().getUniqueUserCount(days);
            System.out.println(mergedCardinality);
        }catch (Exception e){

        }


        UniqueUsersDbStoreV2.getInstance().close();

    }
    static void readTest(){
        String [] days = new String[]{"20210301","20210302","20210304","20210305","20210303"};
        UniqueUsersDbStore.getInstance().init("users-dump-new",8,true);

        try{
            Long mergedCardinality = UniqueUsersDbStore.getInstance().getUniqueUserCount(days);
            System.out.println(mergedCardinality);
        }catch (Exception e){

        }


        UniqueUsersDbStore.getInstance().close();

    }



    public static void main(String [] args){


        //writeTest();
        //readTest();


        //writePrefixTest();
        //readTestPrefixed();
    }
}
