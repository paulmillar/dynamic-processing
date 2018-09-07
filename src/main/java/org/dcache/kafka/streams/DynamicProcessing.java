package org.dcache.kafka.streams;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class DynamicProcessing
{
    private static class Status
    {
        String msg;
        int code;
        public String toString()
        {
            return "[msg:" + msg + ", code:"+code +"]";
        }
    }

    private static class BillingEvent
    {
        String msgType;
        String cellType;
        String transferPath;
        String isWrite;
        boolean isP2p;
        Status status;

        public boolean isEventFromPool()
        {
            return cellType.equals("pool");
        }

        public String getPath()
        {
            return transferPath;
        }

        public boolean isWrite()
        {
            return msgType.equals("transfer") && isWrite.equals("write");
        }

        public boolean isTransferFromClient()
        {
            return !isP2p;
        }

        public boolean isSuccessful()
        {
            return status != null && status.code == 0;
        }

        @Override
        public String toString()
        {
            return "[msgType:" + msgType
                    + ", cellType:" + cellType
                    + ", transferPath:" + transferPath
                    + ", isWrite:" + isWrite
                    + ", isP2p:" + isP2p
                    + ", status:" + status
                    +"]";
        }
    }

    private static class DataToProcessEvent
    {
        String read;
        String write;

        public DataToProcessEvent(String read, String write)
        {
            this.read = read;
            this.write = write;
        }
    }

    private static class MacaroonRequest
    {
        String[] caveats;
        String validity;

        public MacaroonRequest(String... caveats)
        {
            this.caveats = caveats;
        }

        public void setValidity(Duration duration)
        {
            validity = duration.toString();
        }

    }

    private static class MacaroonResponse
    {
        String macaroon;
    }

    private static class IdentityResponse
    {
        String status;
    }

    private static final ContentType MACAROON_REQUEST = ContentType.create("application/macaroon-request");
    private static final Duration MACAROON_VALIDITY = Duration.ofMinutes(5);

    static CloseableHttpClient client = HttpClients.createDefault();
    static UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
            System.getProperty("macaroon.user"), System.getProperty("macaroon.password"));

    public static void main(String[] args) throws Exception
    {
        checkCredentials();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dynamicprocessing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("billing")
                .mapValues(DynamicProcessing::toBillingEvent)
                .filter(DynamicProcessing::onlyInterestingUploads)
                .mapValues(DynamicProcessing::buildNewEvent)
               .to("new-data");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void checkCredentials() throws AuthenticationException, IOException
    {
        HttpGet httpGet = new HttpGet("https://dcache-xdc.desy.de:3880/api/v1/user");
        httpGet.addHeader(new BasicScheme().authenticate(creds, httpGet, null));
        try (CloseableHttpResponse response = client.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Server replied " + response.getStatusLine());
            }
            String result = readEntity(response);
            String status = new Gson().fromJson(result, IdentityResponse.class).status;
            if (status == null || status.equals("ANONYMOUS")) {
                throw new AuthenticationException("Server rejected username+password: " + result);
            }
        }
    }

    private static BillingEvent toBillingEvent(Object value)
    {
        BillingEvent event = new Gson().fromJson(String.valueOf(value), BillingEvent.class);
        System.out.println(event);
        return event;
    }

    private static boolean onlyInterestingUploads(Object key, BillingEvent event)
    {
        return event.isEventFromPool() && event.isWrite() && event.isTransferFromClient()
                && event.isSuccessful() && isInterestingFile(event.getPath());
    }

    private static boolean isInterestingFile(String path)
    {
        return path.startsWith("/Demos/2018-DESY-XDC/Storage-Events/") ||
                path.startsWith("/Demos/2018-DESY-XDC/Rucio-integration/Generated-data");
    }

    private static String buildNewEvent(BillingEvent event)
    {
        String sourcePath = event.getPath();
        String targetPath = buildTargetPath(sourcePath);

        String readUrl = urlWithMacaroon(sourcePath, "DOWNLOAD");
        String writeUrl = urlWithMacaroon(targetPath, "UPLOAD");

        DataToProcessEvent newEvent = new DataToProcessEvent(readUrl, writeUrl);
        return new Gson().toJson(newEvent);
    }

    private static String buildTargetPath(String path)
    {
        File f = new File(path);
        String parent = f.getParent() + "/";
        String name = f.getName();
        int index = name.lastIndexOf('.');
        if (index > 0) {
            return parent + name.substring(0, index) + "-derived" + name.substring(index);
        } else {
            return parent + name + "-derived";
        }
    }

    private static String urlWithMacaroon(String path, String activity)
    {
        String extra;
        try {
            extra = "?authz=" + getMacaroon(path, activity);
        } catch (IOException | AuthenticationException e) {
            System.out.println("Error: " + e + ", carrying on without macaroon");
            extra = "";
        }
        return "https://dcache-xdc.desy.de" + path + extra;
    }

    private static String getMacaroon(String path, String activity) throws AuthenticationException, IOException
    {
        Gson gson = new Gson();
        HttpPost httpPost = new HttpPost("https://dcache-xdc.desy.de" + path);

        MacaroonRequest request = new MacaroonRequest("activity:" + activity);
        request.setValidity(MACAROON_VALIDITY);
        String json = gson.toJson(request);
        httpPost.setEntity(new StringEntity(json, MACAROON_REQUEST));
        httpPost.addHeader(new BasicScheme().authenticate(creds, httpPost, null));

        MacaroonResponse mr;
        try (CloseableHttpResponse response = client.execute(httpPost)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Server replied " + response.getStatusLine());
            }
            String result = readEntity(response);
            return gson.fromJson(result, MacaroonResponse.class).macaroon.trim();
        }
    }

    private static String readEntity(HttpResponse response) throws IOException
    {
        return new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                .lines().collect(Collectors.joining("\n"));
    }
}
