# Dynamic Data

Dynamic data is about having computational resources reacting to new
data.

Here are some example uses:
 *  Harmonising metadata to be consistent,
 *  Uploading metadata from the new data into a catalogue,
 *  Generating derived or summary data.

To achieve this, a process listens on the Kafka 'billing' topic for
the pool reporting new files.  These reports are checked against rules
to see if any activity should be triggered.  If the new file matches a
rule then an event is sent on a Kafka topic.

The event contains URLs for accessing the data.  The URLs are
presigned with macaroons, to allow limited access for a limited
period.  Therefore, the agent processing the request does not need any
special credentials to access the data or upload the results.

# Using the software

To use the software, just run the jar file:

```
java -jar dynamic-processing-0.1-jar-with-dependencies.jar
```

The behaviour is controlled by a configuration file.  By default, this
is the file `config.yaml` in the current directory, but may be
configured using `configuration.path` property; e.g.,

```
java -Dconfiguration.path=/usr/local/etc/dd-config.yaml /usr/local/lib/dynamic-processing-0.1-jar-with-dependencies.jar
```