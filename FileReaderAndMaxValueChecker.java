import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by AK2018 on 4/18/2018.
 */
public class FileReaderAndMaxValueChecker {

    static Map<String, BigDecimal> maxResultCUSIPMap;
    static Map<String, BigDecimal> lastResultCUSIPMap;


    public FileReaderAndMaxValueChecker(Map<String, BigDecimal> maxResultCUSIPMap, Map<String, BigDecimal> lastResultCUSIPMap) {
        this.maxResultCUSIPMap = maxResultCUSIPMap;
        this.lastResultCUSIPMap = lastResultCUSIPMap;
    }

    public static void main(String[] args) {

        new FileReaderAndMaxValueChecker(
                new TreeMap<String, BigDecimal>(),
                new TreeMap<String, BigDecimal>())
                .fileReaderAndMapCreator();

        Iterator max = maxResultCUSIPMap.entrySet().iterator();
        System.out.println("**** Sorted CUSIP and Maximum value for a given Index...");
        while (max.hasNext()) {
            Map.Entry pair = (Map.Entry)max.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            max.remove();
        }

        Iterator last = lastResultCUSIPMap.entrySet().iterator();
        System.out.println("**** Sorted CUSIP and Last known value for a given Index...");
        while (last.hasNext()) {
            Map.Entry pair = (Map.Entry)last.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            last.remove();
        }
    }

    // There might be a way to filter streams -multiple filters- using Lambdas OR stream object
    // But not finding an easy path, so storing the stream into an array
    // then creating a HashMap with key as CUSIP index
    // and value as maximum value for the given index

    // CAUTION, CAUTION
    // This MAY NOT fit in memory as the array size
    // is dependent on the JVM memory allocation.
    // The best way to fit in memory will be to stream
    // for a predetermined size of the array
    // and then remove the elements and continue the loop.
    // I am not adding those conditions as its too much work at this point.

    void fileReaderAndMapCreator(){
        //Unsorted text file with CUSIP indexes and values.
        String cusipFile = "C:\\dev\\workspace\\ICE_NYSE\\Main\\CUSIP.TXT";
        List<String> nameCUSIP = new ArrayList<>();

        try (Stream<String> stream = Files.lines(Paths.get(cusipFile))) {

            // Should use all the processors available...
            nameCUSIP = stream.parallel().collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }
        String tempString = new String();
        BigDecimal tempMaxValue = new BigDecimal(0);
        int result;

        // Find the maximum CUSIP value for each index
        for (String CUSIPValue : nameCUSIP) {
            if (CUSIPValue.startsWith("CUSIP") && !tempString.equalsIgnoreCase(CUSIPValue)) {
                tempMaxValue = new BigDecimal(0);
                tempString = CUSIPValue;
            } else {
                BigDecimal tempValue = new BigDecimal(CUSIPValue);
                result = tempValue.compareTo(tempMaxValue);
                if (result == 0 || result == 1) {
                    tempMaxValue = tempValue;
                }
                maxResultCUSIPMap.put(tempString, tempMaxValue);
            }
        }

        // Find the last CUSIP value for each index.
        BigDecimal tempLastValue = new BigDecimal(0);
        for (String CUSIPValue : nameCUSIP) {
            if (CUSIPValue.startsWith("CUSIP") && !tempString.equalsIgnoreCase(CUSIPValue)) {
                tempLastValue = new BigDecimal(0);
                tempString = CUSIPValue;
            } else {
                tempLastValue = new BigDecimal(CUSIPValue);
                lastResultCUSIPMap.put(tempString, tempLastValue);
            }
        }
    }
}
