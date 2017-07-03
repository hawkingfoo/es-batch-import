package data;

import elasticsearch.ESClient;
import elasticsearch.ESConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.update.UpdateRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * Created by hawkingfoo on 2017/7/3 0003.
 */
public class ESImporter {
    private static final Logger logger = LogManager.getLogger(ESImporter.class);

    public void importer(ESConfig esConfig) {

        File file = new File(esConfig.getFilePath());
        BufferedReader reader = null;
        // 创建BulkProcessor
        BulkProcessor bulkProcessor = new ESClient().createBulkProcessor(esConfig);
        if (bulkProcessor == null) {
            logger.error("create bulk processor failed.");
            return;
        }
        UpdateRequest updateRequest;
        String[] arrStr;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                arrStr = tempString.split("\t");
                if (arrStr.length != 2) {
                    continue;
                }
                updateRequest = new UpdateRequest(esConfig.getEsIndex(), esConfig.getEsType(), arrStr[0])
                        .doc(arrStr[1]).docAsUpsert(true);
                bulkProcessor.add(updateRequest);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (bulkProcessor != null) {
                    bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
                }
            } catch (Exception e) {
                // do nothing
            }
        }
    }
}
