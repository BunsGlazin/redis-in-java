package redis.commands;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

import redis.Database;
import redis.RespWriter;
import redis.Value;

public interface Command {
    void execute(Database db, RespWriter writer, BufferedWriter out, List<Value> args) throws IOException;
}