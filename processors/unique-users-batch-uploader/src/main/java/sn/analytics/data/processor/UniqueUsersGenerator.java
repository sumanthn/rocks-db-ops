package sn.analytics.data.processor;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import sn.analytics.data.store.UniqueUsersDbStoreV2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/** Generate random user per day for over 3 months */
public class UniqueUsersGenerator {

    public static void main(String [] args){
        UniqueUsersDbStoreV2.getInstance().init("users-dump-new",8);

        int maxUsers=Integer.valueOf(args[0]);

        BufferedWriter writer  = null;
        try {
           writer = new BufferedWriter(new FileWriter("stats.csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        DateTime tsEnd = DateTime.parse("2021-07-01", DateTimeFormat.forPattern("yyyy-MM-dd"));
        DateTime ts = tsEnd.minusDays(90);
        Random r = new Random(20210721);
        while(ts.isBefore(tsEnd)){
            String day = ts.toString(DateTimeFormat.forPattern("yyyy-MM-dd"));

            for(int regionId =1000;regionId<1010;regionId++) {
                int maxLimit = r.nextInt(maxUsers - maxUsers / 2) + maxUsers / 2;
                //System.out.println("max limit " + maxLimit);
                Set<Long> ids = new HashSet<>();
                for (int i = 0; i < maxLimit; i++) {
                    long id = (long) r.nextInt(300000000);
                    ids.add(id);
                }


                String key = regionId+":"+day.replaceAll("-","");
                UniqueUsersDbStoreV2.getInstance().addUniqueUsers(key, ids);
                try {
                    writer.write(regionId+":"+day + "," + ids.size());
                    writer.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }//for region ids

            ts = ts.plusDays(1);

        }

        UniqueUsersDbStoreV2.getInstance().close();
        try {
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
