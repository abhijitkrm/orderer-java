// The README quick start, runnable: java -cp out:out-test io.github.abhijitkrm.orderer.Quickstart
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.OType;
import io.github.abhijitkrm.matcher.Types.Side;
import io.github.abhijitkrm.matcher.Types.Tif;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Quickstart {
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("orderer-quickstart");
        Egress.Collect events = Egress.collect(true);
        try (Pipeline<Core.FifoCore> p = Pipeline.builder()
                .partitions(2)
                .journal(Journal.Config2.of(dir, Journal.Format.Binary))  // durable: fsync every 1024 records
                .egress(events.factory())  // or acks(...), metrics(...), callback(...), your own Egress
                .build()) {
            Pipeline.Handle h = p.handle();  // one per thread; publish from any thread
            h.publish(7, new Command.New(1, Side.Ask, OType.Limit, 100, 10, Tif.Gtc));
            h.publish(7, new Command.New(2, Side.Bid, OType.Limit, 100, 4, Tif.Gtc));
            p.drain();  // applied and delivered
            p.snapshot().write(dir.resolve("books.snap"));  // consistent cut: matcher-snap/1 + .meta
        }  // close() = shutdown(): drains, joins every thread, syncs journals
        System.out.print(events.handle().listing());
        System.out.print(Files.readString(dir.resolve("books.snap")));
    }
}
