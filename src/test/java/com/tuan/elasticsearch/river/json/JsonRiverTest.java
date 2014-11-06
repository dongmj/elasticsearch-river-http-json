package com.tuan.elasticsearch.river.json;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class JsonRiverTest {

    private Node node;
    private Client client;

    @Before
    public void setup() throws Exception {
        Spark.post(new Route("/goods1/_search") {
			@Override
			public Object handle(Request req, Response resp) {
				String param = req.body();
				try {
					XContentParser parser = JsonXContent.jsonXContent.createParser(param);
					Map<String, Object> paramsMap = parser.map();
					Integer from = (Integer) paramsMap.get("from");
					Integer size = (Integer) paramsMap.get("size");
					assertThat("size must not be null", size != null);
					String result;
					if(from == 2) {
						result = jsonBuilder().startObject()
								.field("took").value(2)
								.field("time_out").value(false)
								.field("_shards")
									.startObject()
									.field("total").value(5)
									.field("successfull").value(5)
									.field("failed").value(0)
									.endObject()
								.field("hits")
									.startObject()
									.field("total").value(4)
									.field("max_score").value(1.0)
									.field("hits")
										.startArray()
											.startObject()
												.field("_index").value("goods3")
												.field("_type").value("goods_type_3")
												.field("_id").value("3")
												.field("_score").value(1.0)
												.field("_source")
													.startObject()
														.field("GoodsID").value(3)
														.field("GoodsName").value("商品3")
														.field("Price").value(12.0)
													.endObject()
											.endObject()
											.startObject()
												.field("_index").value("goods1")
												.field("_type").value("goods_type_4")
												.field("_id").value("4")
												.field("_score").value(0.6)
												.field("_source")
													.startObject()
														.field("GoodsID").value(4)
														.field("GoodsName").value("商品4")
														.field("Price").value(12.65)
													.endObject()
											.endObject()
										.endArray()
									.endObject()
							.endObject().string();
					} else if(from == 0) {
						result = jsonBuilder().startObject()
								.field("took").value(2)
								.field("time_out").value(false)
								.field("_shards")
									.startObject()
									.field("total").value(5)
									.field("successfull").value(5)
									.field("failed").value(0)
									.endObject()
								.field("hits")
									.startObject()
									.field("total").value(4)
									.field("max_score").value(1.0)
									.field("hits")
										.startArray()
											.startObject()
												.field("_index").value("goods1")
												.field("_type").value("goods_type")
												.field("_id").value("1")
												.field("_score").value(1.0)
												.field("_source")
													.startObject()
														.field("GoodsID").value(1)
														.field("GoodsName").value("商品1")
														.field("Price").value(12.0)
													.endObject()
											.endObject()
											.startObject()
												.field("_index").value("goods1")
												.field("_type").value("goods_type")
												.field("_id").value("2")
												.field("_score").value(0.6)
												.field("_source")
													.startObject()
														.field("GoodsID").value(2)
														.field("GoodsName").value("商品2")
														.field("Price").value(12.65)
													.endObject()
											.endObject()
										.endArray()
									.endObject()
							.endObject().string();						
					} else {
						result = jsonBuilder().startObject()
								.field("took").value(2)
								.field("time_out").value(false)
								.field("_shards")
									.startObject()
									.field("total").value(5)
									.field("successfull").value(5)
									.field("failed").value(0)
									.endObject()
								.field("hits")
									.startObject()
									.field("total").value(4)
									.field("max_score").value(1.0)
									.field("hits")
										.startArray()
										.endArray()
									.endObject()
							.endObject().string();												
					}
					System.out.println(result);
					return result;
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
        });

        String randStr = "UnitTestCluster" + Math.random();
        Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("cluster.name", randStr);

        node = NodeBuilder.nodeBuilder().settings(settingsBuilder).node();
        client = node.client();

        client.prepareIndex("_river", "json1", "_meta").setSource("{ \"type\": \"json\",\"configuration\":{\"sourceURL\":\"http://localhost:4567\",\"inputIndex\":[\"goods1\"],\"outputIndex\":[\"goods\"],\"searchSize\":2} }").execute().actionGet();
        client.admin().indices().prepareCreate("goods").execute().actionGet();

        client.admin().cluster().prepareHealth("goods").setWaitForYellowStatus().execute().actionGet();
        client.admin().cluster().prepareHealth("_river").setWaitForYellowStatus().execute().actionGet();

        client.admin().indices().prepareRefresh("goods").execute().actionGet();
        
        // sleep to ensure that the river has slurped in the data
        Thread.sleep(2000);
        
        client.prepareIndex("_river", "json2", "_meta").setSource("{ \"type\": \"json\",\"configuration\":{\"sourceURL\":\"http://localhost:4567\",\"inputIndex\":[\"goods1\"],\"searchSize\":2} }").execute().actionGet();
        client.admin().indices().prepareCreate("goods1").execute().actionGet();
        Thread.sleep(2000);
    }
    
    @After
    public void closeElasticsearch() {
        client.close();
        node.close();
    }


    @Test
    public void testThatIndexingWorks() throws Exception {
        GetRequestBuilder builder = new GetRequestBuilder(client);
        GetResponse response = builder.setIndex("goods").setType("goods_type").setId("1").execute().actionGet();

        assertThat(response.isExists(), is(true));
        
        response = builder.setIndex("goods").setType("goods_type").setId("2").execute().actionGet();
        assertThat(response.isExists(), is(true));

        response = builder.setIndex("goods1").setType("goods_type").setId("2").execute().actionGet();
        assertThat(response.isExists(), is(true));
        
        response = builder.setIndex("goods1").setType("goods_type_4").setId("4").execute().actionGet();
        assertThat(response.isExists(), is(true));
 
        response = builder.setIndex("goods3").setType("goods_type_3").setId("3").execute().actionGet();
        assertThat(response.isExists(), is(true));
 
        response = builder.setIndex("goods").setType("goods_type_4").setId("4").execute().actionGet();
        assertThat(response.isExists(), is(true));
        
        response = builder.setIndex("goods").setType("goods_type_3").setId("3").execute().actionGet();
        assertThat(response.isExists(), is(true));
        
        client.admin().indices().prepareDelete("goods","goods1").execute().actionGet();
    }

}
