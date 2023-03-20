package com.azure.adx.client;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class ClientUtils {

    public static String endpoint = "https://s.kusto.windows.net";
    public static String tenantID = "";
    public static String clientID = "";
    public static String clientSecret = "";
    public static String database = null;

    public static Client getClient() throws URISyntaxException {
        //
        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCredentials(endpoint,
                        clientID,
                        clientSecret,
                        tenantID);
        //
        HttpClientProperties properties = HttpClientProperties.builder()
                .keepAlive(true)
                .maxKeepAliveTime(120)
                .maxConnectionsPerRoute(40)
                .maxConnectionsTotal(40)
                .build();
        //
        Client client = ClientFactory.createClient(csb, properties);
        return client;
    }

}
