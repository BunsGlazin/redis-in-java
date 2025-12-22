package redis.resp;

import java.io.BufferedWriter;
import java.io.IOException;

public class RespWriter {
    public void writeSimple(BufferedWriter out, String s) throws IOException {
        out.write("+" + s + "\r\n");
    }

    public void writeError(BufferedWriter out, String s) throws IOException {
        out.write("-ERR " + s + "\r\n");
    }

    public void writeBulk(BufferedWriter out, String s) throws IOException {
        if (s == null) {
            out.write("$-1\r\n");
            return;
        }
        out.write("$" + s.length() + "\r\n" + s + "\r\n");
    }

    public void writeInt(BufferedWriter out, long n) throws IOException {
        out.write(":" + n + "\r\n");
    }

    public void writeArrayHeader(BufferedWriter out, int n) throws IOException {
        out.write("*" + n + "\r\n");
    }
}
