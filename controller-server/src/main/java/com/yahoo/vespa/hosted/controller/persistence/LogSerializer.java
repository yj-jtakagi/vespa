package com.yahoo.vespa.hosted.controller.persistence;

import com.yahoo.slime.ArrayTraverser;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Inspector;
import com.yahoo.slime.ObjectTraverser;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.config.SlimeUtils;
import com.yahoo.vespa.hosted.controller.api.integration.LogEntry;
import com.yahoo.vespa.hosted.controller.api.integration.LogEntry.Type;
import com.yahoo.vespa.hosted.controller.deployment.Step;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialisation of LogRecord objects. Not all fields are stored!
 *
 * @author jonmv
 */
class LogSerializer {

    private static final String idField = "id";
    private static final String levelField = "level";
    private static final String typeField = "type";
    private static final String timestampField = "at";
    private static final String messageField = "message";

    byte[] toJson(Map<Step, List<LogEntry>> log) {
        try {
            return SlimeUtils.toJsonBytes(toSlime(log));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Slime toSlime(Map<Step, List<LogEntry>> log) {
        Slime root = new Slime();
        Cursor logObject = root.setObject();
        log.forEach((step, entries) -> {
            Cursor recordsArray = logObject.setArray(RunSerializer.valueOf(step));
            entries.forEach(entry -> toSlime(entry, recordsArray.addObject()));
        });
        return root;
    }

    private void toSlime(LogEntry entry, Cursor entryObject) {
        entryObject.setLong(idField, entry.id());
        entryObject.setLong(timestampField, entry.at());
        entryObject.setString(levelField, valueOf(entry.type())); // TODO jvenstad: Remove after one deployment.
        entryObject.setString(typeField, valueOf(entry.type()));
        entryObject.setString(messageField, entry.message());
    }

    Map<Step, List<LogEntry>> fromJson(byte[] logJson, long after) {
        return fromJson(Collections.singletonList(logJson), after);
    }

    Map<Step, List<LogEntry>> fromJson(List<byte[]> logJsons, long after) {
        return fromSlime(logJsons.stream()
                                 .map(SlimeUtils::jsonToSlime)
                                 .collect(Collectors.toList()),
                         after);
    }

    Map<Step, List<LogEntry>> fromSlime(List<Slime> slimes, long after) {
        Map<Step, List<LogEntry>> log = new HashMap<>();
        slimes.forEach(slime -> slime.get().traverse((ObjectTraverser) (stepName, entryArray) -> {
            Step step = RunSerializer.stepOf(stepName);
            List<LogEntry> entries = log.computeIfAbsent(step, __ -> new ArrayList<>());
            entryArray.traverse((ArrayTraverser) (__, entryObject) -> {
                LogEntry entry = fromSlime(entryObject);
                if (entry.id() > after)
                    entries.add(entry);
            });
        }));
        return log;
    }

    private LogEntry fromSlime(Inspector entryObject) {
        return new LogEntry(entryObject.field(idField).asLong(),
                            entryObject.field(timestampField).asLong(),
                            entryObject.field(typeField).valid() // TODO jvenstad: Remove after one deployment.
                                    ? typeOf(entryObject.field(typeField).asString())
                                    : typeOf(entryObject.field(levelField).asString()),
                            entryObject.field(messageField).asString());
    }

    static String valueOf(Type type) {
        switch (type) {
            case debug: return "debug";
            case info: return "info";
            case warning: return "warning";
            case error: return "error";
            case html: return "html";
            default: throw new AssertionError("Unexpected log entry type '" + type + "'!");
        }
    }

    static Type typeOf(String type) {
        switch (type.toLowerCase()) { // TODO jvenstad: Remove lowercasing after this has been deployed.
            case "debug": return Type.debug;
            case "info": return Type.info;
            case "warning": return Type.warning;
            case "error": return Type.error;
            case "html": return Type.html;
            default: throw new IllegalArgumentException("Unknown log entry type '" + type + "'!");
        }
    }

}