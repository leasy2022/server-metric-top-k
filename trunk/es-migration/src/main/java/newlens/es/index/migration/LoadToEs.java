package newlens.es.index.migration;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadToEs {
	private final static Logger logger = LoggerFactory.getLogger(LoadToEs.class);
	final private String destIndex;
	final private String type;
	final private String fileName;
	final private String esHosts;
	final String esClusterName;
	
	public LoadToEs(String esHosts, String esClusterName, String destIndex, String type, String fileName) {
		this.type = type;
		this.destIndex = destIndex;
		this.fileName = fileName;
		this.esHosts = esHosts;
		this.esClusterName = esClusterName;
	}
	
	
	public void load() {
		Client client = EsClientSingleton.getEsClient(esClusterName, esHosts);
		try {
			//读取刚才导出的ES数据
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String json = null;
			int count = 0;
			//开启批量插入
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			while ((json = br.readLine()) != null) {
				bulkRequest.add(client.prepareIndex(destIndex, type).setSource(json));
				//每一千条提交一次
				if (count != 0 && count % 1000 == 0) {
					bulkRequest.execute().actionGet();
					System.out.println("提交了：" + count);
					logger.debug("documents commited, count=" + count);
				}
				count++;
			}
			bulkRequest.execute().actionGet();
			logger.info("all documents commited to es:" + destIndex + "/" + type + " , count=" + count);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
	}
	
	public static void main(String[] args) {
		String esHost = "192.168.1.129";
		String esClusterName = "es-cluster";
		String destIndex = "tingyun-dc-201608-v1";
		String type = "nl_mob_app_anr_data";
		String fileName = "/Users/wushang/IdeaProjects/tingyun-dc-app-es-api/trunk/es";
		if (args.length > 0) {
			esHost = args[0];
			esClusterName = args[1];
			destIndex = args[2];
			type = args[3];
			fileName = args[4];
		}

		System.out.println("esHost=" + esHost + ", esClusterName=" + esClusterName + ", destIndex=" + destIndex + ", type=" + type + ", fileName=" + fileName);
		LoadToEs loadData = new LoadToEs(esHost, esClusterName, destIndex, type, fileName);
		loadData.load();
	}
}
