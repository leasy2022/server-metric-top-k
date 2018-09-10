package newlens.es.index.migration;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by wushang on 16/11/8.
 */
public class EsClientSingleton {
	private static Client client;


	private EsClientSingleton() {

	}

	public static Client getEsClient(String clusterName, String esHosts) {

		if(client == null) {
			synchronized (EsClientSingleton.class) {
				if(client == null) {
					Settings settings = Settings.settingsBuilder()
							                    .put("cluster.name", clusterName)
							                    .put("client.transport.sniff", true).build();
					TransportClient transportClient = TransportClient.builder().settings(settings).build();
					String[] hostNames = esHosts.split(",", -1);
					for (String hostName : hostNames) {
						try {
							client = transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), 9300));
						} catch (UnknownHostException e) {
							e.printStackTrace();
						}
					}
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return client;
	}

}
