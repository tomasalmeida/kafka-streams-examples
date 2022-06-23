/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;


/**
 * Demonstrates how kafka stream join works
 * <p>
 * In this simple example, we create a new topic PLAYING_IN_COUNTRY where we show the current song played in a given country
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start the containers
 * $ docker-compose up -d kafka kafka-create-topics schema-registry zookeeper
 * <p>
 * 2) Start the application producer either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-7.1.1-standalone.jar \
 *      io.confluent.examples.streams.interactivequeries.kafkamusic.KafkaRandomMusicExampleDriver
 * }
 * </pre>
 * 3) Start this example application kstream either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-7.1.1-standalone.jar io.confluent.examples.streams.CountrySongExample
 * }
 * </pre>
 * 4) Check the result in the topic
 * <pre>
 * {@code
 * $ kafka-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 \
 *  --property print.key=true --key-deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 * --topic playing-in-country --from-beginning
 * }
 * </pre>
 * ✗
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 * <pre>
 * {@code
 * $  docker-compose down -v
 * }
 * </pre>
 * <p>
 * You can also take a look at io.confluent.examples.streams.SessionWindowsExampleTest for an example
 * of the expected outputs.
 */
public class CountrySongExample {
    static final String PLAY_EVENTS = "play-events";
    static final String SONG_FEED = "song-feed";
    static final String PLAYING_IN_COUNTRY = "playing-in-country";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final Topology topology = buildTopology(serdeConfig);
        System.out.println(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig(bootstrapServers, "/tmp/kafka-streams")
        );

        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("<<< Stopping the streams-app Application");
            streams.close();
        }));
    }

    static Properties streamsConfig(final String bootstrapServers, final String stateDir) {
        final Properties config = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "country-songs");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "contry-songs-stream");
        // Where to find Kafka broker(s).
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // Set to earliest so we don't miss any data that arrived in the topics before the process
        // started
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // disable caching to see session merging
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return config;
    }

    static Topology buildTopology(final Map<String, String> serdeConfig) {
        final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
        playEventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Song> songSerde = new SpecificAvroSerde<>();
        songSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> streamSongPlayPerCountry = builder
                .stream(PLAY_EVENTS, Consumed.with(Serdes.String(), playEventSerde))
                .map((country, playEvent) -> KeyValue.pair(playEvent.getSongId(), country));

        final KTable<Long, String> songNameKTable = builder
                .stream(SONG_FEED, Consumed.with(Serdes.Long(), songSerde))
                .mapValues(Song::getName)
                .toTable();


        streamSongPlayPerCountry
                .join(songNameKTable, (country, songName) -> Pair.of(country, songName))
                .map((songId, countrySongPair) -> KeyValue.pair(countrySongPair.getLeft(), countrySongPair.getRight()))
                .peek(((key, value) -> System.out.println("["+key+"]:["+value+"]")))
                .to(PLAYING_IN_COUNTRY, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

}
