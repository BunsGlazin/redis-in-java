package redis.resp;

import java.util.List;

public class Value {
    public String typ;
    public String str;
    public List<Value> array;

    @SuppressWarnings("unchecked")
    public Value(String typ, Object data) {
        this.typ = typ;
        if ("array".equals(typ)) {
            this.array = (List<Value>) data;
        } else if (data != null) {
            this.str = data.toString();
        }
    }

    @Override
    public String toString() {
        if ("array".equals(typ)) {
            return array.toString();
        }
        return str;
    }
}