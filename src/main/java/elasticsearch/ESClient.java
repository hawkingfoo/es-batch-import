package elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hawkingfoo on 2017/7/3 0003.
 */
public class ESClient {
    private static final Logger logger = LogManager.getLogger(ESClient.class);

    public BulkProcessor createBulkProcessor(ESConfig esConfig) {
        String clusterName = esConfig.getEsClusterName();
        String clusterAddr = esConfig.getEsClusterAddress();

        if (clusterName == null || clusterName.isEmpty()) {
            logger.error("invalid cluster name.");
            return null;
        }
        if (clusterAddr == null || clusterAddr.isEmpty()) {
            logger.info("invalid cluster address.");
            return null;
        }
        String[] addr = clusterAddr.split(":");
        if (addr.length != 2) {
            logger.info("invalid cluster address.");
            return null;
        }
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("cluster.transport.sniff", true)
                .put("index.refresh_interval", "60s")
                .build();
        // 创建 TransportClient
        TransportClient transportClient = new TransportClient.Builder()
                .settings(settings).build();

        List<InetSocketTransportAddress> addrList = new ArrayList<>();
        try {
            addrList.add(new InetSocketTransportAddress(InetAddress.getByName(addr[0]),
                    Integer.parseInt(addr[1])));
        } catch (Exception e) {
            logger.error("exception:", e);
            return null;
        }

        for (InetSocketTransportAddress address : addrList) {
            transportClient.addTransportAddress(address);
        }
        Client client = transportClient;

        // 初始化Bulk处理器
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    long begin;
                    long cost;
                    int count = 0;

                    @Override
                    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                        begin = System.currentTimeMillis();
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        cost = (System.currentTimeMillis() - begin) / 1000;
                        count += bulkRequest.numberOfActions();
                        logger.info("bulk success. size:[{}] cost:[{}s]", count, cost);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                        logger.error("bulk update has failures, will retry:" + throwable);
                    }
                })
                .setBulkActions(esConfig.getBatchSize())                    // 批量导入个数
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))    // 满1MB进行导入
                .setConcurrentRequests(esConfig.getEsThreadNum())           // 并发数
                .setFlushInterval(TimeValue.timeValueSeconds(5))            // 冲刷间隔60s
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3)) // 重试3次，间隔1s
                .build();
        return bulkProcessor;
    }
}
