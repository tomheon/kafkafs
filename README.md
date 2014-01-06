# Kafkafs: a FUSE module for exploring / consuming a Kafka cluster in a filesystem

This project intended for command line exploration / debugging of
Kafka messages, rather than as a consumer of a stream of messages,
though it's possible to use it as a consumer (see "Using as a
Consumer" below).

## Building

Make sure fuse is installed, then:

    go get github.com/Shopify/sarama
    go get github.com/hanwen/go-fuse/fuse
    go build kafkafsmount.go

## Use

### Running

    ./kafkafsmount --kafkaAddrs <kafkahost:kafkaport> </your/mount/point>

Ctrl-c will quit and unmount if possible (if a client is still in the
mount point, you'll have to fusermount -u later).

### Consuming / Exploring

Then

    cd /your/mount/point

and

    ls

You'll see a number of directories, each named for a Kafka topic.
Cd'ing into a topic directory and ls'ing will show you one directory
for each partition in the topic.  Cd'ing into a partition directory
and ls'ing will show you a number of files named for message offsets,
which is where it gets interesting.  Say for example:

    cd /your/mount/point/sometopic/0
    ls

Gave you the output

    0 199841

From this you could divine that:

* the earliest message offset still available in partition 0 of
  sometopic is 0

* the latest message offset currently available in partition 0 of
  sometopic is 199841

Each of these files contains the bytes of the message at that offset,
so for example

   cat 199841

will print the latest message to the console.

If you leave the /your/mount/point/sometopic/0 directory and cd back,
the local data will be refreshed, and you may find that the contents
of the directory have changed because older offsets have expired or
newer offsets have appeared.

## Getting arbitrary offsets

Resuming our example from above,

    cd /your/mount/point/sometopic/0
    ls

Gave you the output

    0 199841

But let's say you want to see the message at offset 101.  Just ```cat
101``` it, as if it existed, and kafkafs will fetch it from Kafka for
you.  If the offset does not yet exist (for example, if you ```cat
199842```), the call will block until the message becomes available or
one second has passed (at which point it fails and you can try again).

Once you request an arbitrary offset, it will remain in the fs until
you ```rm``` it (unlike the earliest and latest offsets, which are
automatically refreshed on each directory open).

## Using as a Consumer

It's possible to use kafkafs as an ad-hoc consumer of a kafka message
stream, if, for some reason, you don't want to use a Kafka client
library.  For example, in python, something like:

    os.chdir("/your/mount/point/sometopic/0")
    next_offset = max([int(offset) for offset in os.listdir(".")]) + 1
    while True:
        try:
            # blocks until next message is available or 1 second has passed
            f = open("/your/mount/point/sometopic/0/%d" % next_offset, 'rb')
            consume(f.read())
            f.close()
            os.unlink("/your/mount/point/sometopic/0/%d" % next_offset)
            next_offset += 1
        except IOError:
            # a second passed with no new message, loop
            pass


All the fs operations are going to make this more costly than a normal
Kafka client, however, so you'll have to make sure your consumer can
actually keep up.

## Bugs

Report them here at Github.

## Contributions

Welcome.
