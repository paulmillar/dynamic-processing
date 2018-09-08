package org.dcache.kafka.streams;

import com.google.gson.Gson;
import org.apache.http.auth.AuthenticationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.dcache.kafka.streams.billing.BillingEvent;
import org.dcache.macroons.MacaroonClient;

public class DynamicProcessing
{
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

    private static MacaroonClient macaroons;

    public static void main(String[] args) throws Exception
    {
        macaroons = new MacaroonClient(System.getProperty("macaroon.user"), System.getProperty("macaroon.password"));

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
            extra = "?authz=" + macaroons.getMacaroon(path, activity);
        } catch (IOException | AuthenticationException e) {
            System.out.println("Error: " + e + ", carrying on without macaroon");
            extra = "";
        }
        return "https://dcache-xdc.desy.de" + path + extra;
    }
}
