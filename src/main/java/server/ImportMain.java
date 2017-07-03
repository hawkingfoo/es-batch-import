package server;

import data.ESImporter;
import elasticsearch.ESConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by hawkingfoo on 2017/7/3 0003.
 */
public class ImportMain {
    private static final Logger logger = LogManager.getLogger(ImportMain.class);

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                System.err.println("usage: <file_path>");
                System.exit(1);
            }
            ESConfig esConfig = new ESConfig()
                    .setEsClusterName("elasticsearch")     // 设置集群名字
                    .setEsClusterAddress("127.0.0.1:9300") // 设置集群地址
                    .setEsIndex("person")
                    .setEsType("infos")
                    .setBatchSize(100)                     // 批量个数
                    .setFilePath(args[0])                  // 读取的文件路径
                    .setEsThreadNum(1);

            long begin = System.currentTimeMillis();
            ESImporter esImporter = new ESImporter();
            esImporter.importer(esConfig);
            long cost = System.currentTimeMillis() - begin;
            logger.info("import end. cost:[{}ms]", cost);
        } catch (Exception e) {
            logger.error("exception:", e);
        }
    }
}
