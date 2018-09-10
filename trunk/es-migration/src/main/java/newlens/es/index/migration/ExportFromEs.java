package newlens.es.index.migration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class ExportFromEs {
	private final static Logger logger = LoggerFactory.getLogger(ExportFromEs.class);

	final private String sourceIndex;
	final private String type;
	final private String destFile;
	final private String esClusterName;
	final private String esHosts;

	public ExportFromEs(String esHosts, String esClusterName, String sourceIndex, String type, String destFile) {
		this.sourceIndex = sourceIndex;
		this.type = type;
		this.destFile = destFile;
		this.esClusterName = esClusterName;
		this.esHosts = esHosts;
	}

	private void export() throws Exception {
		Client client = EsClientSingleton.getEsClient(esClusterName, esHosts);

		SearchResponse response = client.prepareSearch(sourceIndex).setTypes(type)
				                          .setQuery(QueryBuilders.matchAllQuery()).setSize(1000).setScroll(new TimeValue(600000))
				                          .setSearchType(SearchType.SCAN).execute().actionGet();//setSearchType(SearchType.Scan) 告诉ES不需要排序只要结果返回即可 setScroll(new TimeValue(600000)) 设置滚动的时间
		String scrollid = response.getScrollId();
		//把导出的结果以JSON的格式写到文件里
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(destFile, true));
			//每次返回数据10000条。一直循环查询直到所有的数据都查询出来
			while (true) {
				SearchResponse response2 = client.prepareSearchScroll(scrollid).setScroll(new TimeValue(1000000))
						                           .execute().actionGet();
				SearchHits searchHit = response2.getHits();
				//再次查询不到数据时跳出循环
				if (searchHit.getHits().length == 0) {
					break;
				}
//				System.out.println("查询数量 ：" + searchHit.getHits().length);
				logger.debug("documents export, count=" + searchHit.getHits().length);
				for (int i = 0; i < searchHit.getHits().length; i++) {
					String json = searchHit.getHits()[i].getSourceAsString();
//					System.out.println(json);
					out.write(json);
					out.write("\r\n");
				}
			}
//			System.out.println("查询结束");
			logger.info("All documents export from es:" + sourceIndex + "/" + type);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
	}

	public static void main(String[] args) throws Exception {
		String esHost = "10.194.1.2";
		String esClusterName = "tingyun-app-es";
		String sourceIndex = "tingyun-es-201703";
		String type = "nl_mob_app_anr_data";
		String fileName = "es";
		if (args.length > 0) {
			esHost = args[0];
			esClusterName = args[1];
			sourceIndex = args[2];
			type = args[3];
			fileName = args[4];
		}

		System.out.println("esHost=" + esHost + ", esClusterName=" + esClusterName + ", sourceIndex=" + sourceIndex + ", type=" + type + ", fileName=" + fileName);
		ExportFromEs exportData = new ExportFromEs(esHost, esClusterName, sourceIndex, type, fileName);
		exportData.export();
	}
}
