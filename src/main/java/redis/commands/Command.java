package redis.commands;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

import redis.Database;
import redis.resp.RespWriter;
import redis.resp.Value;

public interface Command {
    void execute(Database db, RespWriter writer, BufferedWriter out, List<Value> args) throws IOException;
}