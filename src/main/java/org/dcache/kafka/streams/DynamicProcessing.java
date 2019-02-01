package org.dcache.kafka.streams;

import com.google.gson.Gson;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import javax.net.ssl.SSLContext;

import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.dcache.http.WebDAVClient;
import org.dcache.kafka.streams.billing.BillingEvent;
import org.dcache.macroons.MacaroonClient;

public class DynamicProcessing
{
    public static class OutgoingEventSerializer implements Serializer<OutgoingEvent>
    {
        @Override
        public void configure(Map map, boolean bln) {
        }

        @Override
        public byte[] serialize(String topic, OutgoingEvent data) {
            return data.getPayload().getBytes();
        }

        @Override
        public void close() {
        }
    }

    public static class OutgoingEventDeserializer implements Deserializer<OutgoingEvent>
    {
        @Override
        public void configure(Map map, boolean bln) {
        }

        @Override
        public void close() {
        }

        @Override
        public OutgoingEvent deserialize(String topic, byte[] bytes) {
            return new OutgoingEvent(topic, new String(bytes, StandardCharsets.UTF_8));
        }
    }

    public static class CustomSerde implements Serde<OutgoingEvent>
    {
        @Override
        public void configure(Map map, boolean bln) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<OutgoingEvent> serializer() {
            return new OutgoingEventSerializer();
        }

        @Override
        public Deserializer<OutgoingEvent> deserializer() {
            return new OutgoingEventDeserializer();
        }
    }

    private static MacaroonClient macaroons;
    private static UrlGenerator urlGenerator;

    private static CloseableHttpClient prepareClient(Configuration config)
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();

        HttpClientBuilder builder = HttpClientBuilder.create();
        SSLConnectionSocketFactory sslConnectionFactory =
                new SSLConnectionSocketFactory(sslContext.getSocketFactory(),
                        new NoopHostnameVerifier());
        builder.setSSLSocketFactory(sslConnectionFactory);
        Registry<ConnectionSocketFactory> registry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                .register("https", sslConnectionFactory)
                .register("http", new PlainConnectionSocketFactory())
                .build();

        Configuration.Macaroons mConfig = config.getUrlGenerator().getMacaroons();
        CredentialsProvider provider = new BasicCredentialsProvider();
        Credentials creds = new UsernamePasswordCredentials(mConfig.getUsername(), mConfig.getPassword());
        provider.setCredentials(AuthScope.ANY, creds);
        builder.setDefaultCredentialsProvider(provider);

        HttpClientConnectionManager ccm = new BasicHttpClientConnectionManager(registry);
        builder.setConnectionManager(ccm);
        return builder.build();
    }

    public static void main(String[] args) throws Exception
    {
        String configPath = System.getProperty("configuration.path", "config.yaml");
        Configuration config = new ConfigurationLoader(configPath).load();
        config.checkValid();

        CloseableHttpClient httpClient = prepareClient(config);

        WebDAVClient webdavClient = new WebDAVClient(httpClient);
        macaroons = new MacaroonClient(config.getUrlGenerator().getMacaroons(), httpClient);
        urlGenerator = new UrlGenerator(config.getUrlGenerator().getBase(), macaroons);

        Configuration.Client client = config.getClient();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, client.getId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, client.getBootstrap());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        EventGenerators generators = new EventGenerators(config.getEventSource(), urlGenerator, webdavClient);

        Serde<OutgoingEvent> valueSerde = new CustomSerde();
        Serde<String> keySerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String,String>stream("billing")
                .mapValues(DynamicProcessing::toBillingEvent)
                .filter(DynamicProcessing::onlyInterestingUploads)
                .peek((k,v) -> {System.out.println("Event: " + v);})
                .mapValues(BillingEvent::getPath)
                .flatMapValues(generators::eventsFor)
                .to((k,v,e) -> v.getTopic(), Produced.with(keySerde, valueSerde));

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
                && event.isSuccessful();
    }
}
