// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.dns;

import java.util.List;
import java.util.Objects;

public class RotationRecord {
    private final RecordId id;
    private final Record.Type type;
    private final RecordName name;
    private final List<RotationData> data;

    public RotationRecord(RecordId id, Record.Type type, RecordName name, List<RotationData> data) {
        this.id = id;
        this.type = type;
        this.name = name;
        this.data = data;
    }

    public RecordId id() {
        return id;
    }

    public Record.Type type() {
        return type;
    }

    public RecordName name() {
        return name;
    }

    public List<RotationData> data() {
        return data;
    }

    @Override
    public String toString() {
        return "RotationRecord{" +
               "id=" + id +
               ", type=" + type +
               ", name=" + name +
               ", data=" + data +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RotationRecord that = (RotationRecord) o;
        return Objects.equals(id, that.id) &&
               type == that.type &&
               Objects.equals(name, that.name) &&
               Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, name, data);
    }
}
