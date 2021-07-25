package sn.analytics.data.processor;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import sn.analytics.data.store.UniqueUsersDbStoreV2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UniqueDataCheck {

    public static void main(String [] args){
        Map<String,Long> countByRegionDay = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("stats.csv"));
            String line = reader.readLine();
            while(line!=null){
                String [] tkns = line.split(",");
                countByRegionDay.put(tkns[0].replaceAll("-",""),Long.valueOf(tkns[1]));

                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        UniqueUsersDbStoreV2.getInstance().init("users-dump-new",8,true);

        DateTime tsEnd = DateTime.parse("2021-07-01", DateTimeFormat.forPattern("yyyy-MM-dd"));
        DateTime ts = tsEnd.minusDays(90);
        while(ts.isBefore(tsEnd)) {
            for(int regionId =1000;regionId<1010;regionId++) {
                String day = ts.toString(DateTimeFormat.forPattern("yyyyMMdd"));

                String k = regionId+":"+day;
                Long val = UniqueUsersDbStoreV2.getInstance().getUniqueUserCount(k);
                if (countByRegionDay.containsKey(k)){
                    Long actual = countByRegionDay.get(k);
                    double diff = (actual-val)*100.d/actual;
                    System.out.println(diff +" for " + k);
                }else{
                    System.out.println("key not found "+ k);
                }
            }
            ts = ts.plusDays(1);
        }

            UniqueUsersDbStoreV2.getInstance().close();

        }

}
