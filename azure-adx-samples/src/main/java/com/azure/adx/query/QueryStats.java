package com.azure.adx.query;

import com.azure.adx.client.ClientUtils;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;

public class QueryStats {
    //
    public static String database = "O2S";
    public static String newLine = System.getProperty("line.separator");

    public static void main(String[] args) throws Exception {
        //
        Client client = ClientUtils.getClient();
        //
        ordersByMembershipId(client);
        ordersByDateRanges(client);
        ordersByOrderNo(client);
        ordersByOrderDescFuzzy(client);
        System.out.println("------------------------------------------------------------------------");

    }

    public static void ordersByOrderDescFuzzy(final Client client) throws Exception {
        //
        long startTime = System.currentTimeMillis();
        //
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = String.join(newLine,
                "Club",
                "| mv-expand OrderLine = OrderLines",
                "| where tostring(OrderLine.itemDesc) contains \"GREEN BEAN\" and MembershipId == '0018157372'",
                "| limit 500");
        KustoOperationResult results = client.execute(database, query, clientRequestProperties);
        KustoResultSetTable mainTableResult = results.getPrimaryResults();
        //
        long endTime = System.currentTimeMillis();
        printStats(query, (endTime-startTime),mainTableResult.count());
    }


    public static void ordersByOrderNo(final Client client) throws Exception {
        //
        long startTime = System.currentTimeMillis();
        //
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = String.join(newLine,
                "Club",
                "| where OrderNo == '000000000000006319189'");
        KustoOperationResult results = client.execute(database, query, clientRequestProperties);
        KustoResultSetTable mainTableResult = results.getPrimaryResults();
        //
        long endTime = System.currentTimeMillis();
        printStats(query, (endTime-startTime),mainTableResult.count());
    }

    public static void ordersByDateRanges(final Client client) throws Exception {
        //
        long startTime = System.currentTimeMillis();
        //
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = String.join(newLine,
                "Club",
                "| where DocType == 'ORDER'",
                "| where OrderDate  between (datetime(2019-12-15 15:15:00) .. datetime(2020-12-31 22:30:00))",
                "| order by OrderDate desc");
        KustoOperationResult results = client.execute(database, query, clientRequestProperties);
        KustoResultSetTable mainTableResult = results.getPrimaryResults();
        //
        long endTime = System.currentTimeMillis();
        printStats(query, (endTime-startTime),mainTableResult.count());
    }

    public static void ordersByMembershipId(final Client client) throws Exception {
        //
        long startTime = System.currentTimeMillis();
        //
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = String.join(newLine,
                "Club",
                "| where MembershipId == '0598136133'");
        KustoOperationResult results = client.execute(database, query, clientRequestProperties);
        KustoResultSetTable mainTableResult = results.getPrimaryResults();
        //
        long endTime = System.currentTimeMillis();
        printStats(query, (endTime-startTime),mainTableResult.count());
        /*
        // Iterate values
        while (mainTableResult.next()) {
            System.out.println("MembershipId "+mainTableResult.getString("MembershipId"));
        }*/
    }

    public static void printStats(final String query, final long ms, final long rowCount) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("\nquery : \n\t"+query.replace("|", "\t|")
                + "\nExecution time : " + ms + " milliseconds"
                + "\nRow count : " + rowCount);
    }

}
