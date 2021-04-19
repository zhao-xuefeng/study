package es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class EsTest {


    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException {
        String host = "";
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(new InetSocketAddress(host, 9300)));
//        Map<String,Object> json=new HashMap<>();
//        json.put("user","kimchy");
//        json.put("postDate",new Date());
//        json.put("message","trying out Elasticsearch");
//        将对象序列化为json
//        ObjectMapper objectMapper=new ObjectMapper();
//        byte[] jsonb=objectMapper.writeValueAsBytes();
//        创建索引
//        client.prepareIndex("person","info").setSource(json, XContentType.JSON);
/**
 * 获取所有index
 */
//        ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
//        IndicesAdminClient indicesAdminClient = client.admin().indices();
//
//        Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
//        Set entries = isr.actionGet().getIndices().entrySet();

////        GetResponse response=client.admin().indices()
//        Map<String,Object> map=response.getSourceAsMap();
////        Map.Entry<String,Object> entry= (Map.Entry) map.entrySet();
//        Set entries=map.entrySet();
//        Iterator iterator = entries.iterator();
//        while (iterator.hasNext()){
//            Map.Entry entry = (Map.Entry) iterator.next( );
//            System.out.println(entry.getKey().toString()+"-------"+entry.getValue());
//        }

//        System.out.println( client.prepareGet().get().getIndex());
//        Set<Map.Entry<String,Object>>entries=client.get(new GetRequest("springboot").id("Oazryvb9Ryy5hn0BrR16_A")).actionGet().getSourceAsMap().entrySet();
//        entries.stream().iterator().forEachRemaining(new Consumer<Map.Entry<String, Object>>() {
//            @Override
//            public void accept(Map.Entry<String, Object> stringObjectEntry) {
//                System.out.println(stringObjectEntry.getKey()+"----"+stringObjectEntry.getValue());
//            }
//        });
//        SearchRequestBuilder sb= client.prepareSearch("springboot");
//        SearchResponse response=sb.setSearchType(SearchType.DEFAULT).get();
//        System.out.println(response.toString());
        String spring = client.prepareGet("springboot", "doc", "HDFTaXYB9QBfYzKqO-NG").get().getSourceAsString();
        System.out.println(spring);
//        AdminClient adminClient = client.admin();
//        ActionFuture<IndicesStatsResponse>  actionFuture=adminClient.indices().stats(new IndicesStatsRequest().all());
//        actionFuture.actionGet().getIndices().forEach(new BiConsumer<String, IndexStats>() {
//            @Override
//            public void accept(String s, IndexStats indexShardStats) {
//                System.out.println(s+"==="+indexShardStats.getIndex());
//            }
//        });
//                getIndex(new GetIndexRequest().indices("springboot"));
//        Map<String,IndexStats> map=actionFuture.actionGet().getIndices();
//        Set entries=map.entrySet();
//        Iterator iterator = entries.iterator();
//        while (iterator.hasNext()){
//            Map.Entry entry = (Map.Entry) iterator.next( );
//            System.out.println(entry.getKey().toString()+"-------"+entry.getValue());
//        }

    }
}

