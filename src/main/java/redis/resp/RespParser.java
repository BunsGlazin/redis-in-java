package redis.resp;

import java.io.*;
import java.util.*;

public class RespParser {
	public static Value readValue(BufferedReader reader) throws IOException, RespParseException {
		int prefix = reader.read();
		if (prefix == -1) {
			return null; // EOF
		}

		switch (prefix) {
			case '*': // Array
				int count = Integer.parseInt(reader.readLine());
				List<Value> elements = new ArrayList<>();
				for (int i = 0; i < count; i++) {
					elements.add(readValue(reader));
				}
				return new Value("array", elements);

			case '$': // Bulk string
				int length = Integer.parseInt(reader.readLine());
				if (length == -1)
					return new Value("null", null); // Null bulk
				char[] buf = new char[length];
				reader.read(buf, 0, length);
				reader.readLine(); // read CRLF
				return new Value("bulk", new String(buf));

			case '+': // Simple string
				return new Value("string", reader.readLine());

			case ':': // Integer
				return new Value("integer", reader.readLine());

			case '-': // Error
				return new Value("error", reader.readLine());

			default:
				throw new RespParseException("Unknown RESP prefix: " + (char) prefix);
		}
	}
}
