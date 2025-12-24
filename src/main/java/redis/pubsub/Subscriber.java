package redis.pubsub;

import java.io.BufferedWriter;

public class Subscriber {
    public final BufferedWriter out;
    public final Object lock; // For thread-safe writes

    public Subscriber(BufferedWriter out) {
        this.out = out;
        this.lock = new Object();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Subscriber))
            return false;
        return out.equals(((Subscriber) o).out);
    }

    @Override
    public int hashCode() {
        return out.hashCode();
    }
}