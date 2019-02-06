// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.dns;

import com.yahoo.vespa.hosted.controller.api.integration.zone.ZoneId;

import java.util.Objects;

public class RotationData {

    private final String targetName;
    private final ZoneId zoneId;
    private final String dnsZone;

    public RotationData(String targetName, ZoneId zoneId, String dnsZone) {
        this.targetName = targetName;
        this.zoneId = zoneId;
        this.dnsZone = dnsZone;
    }

    public String targetName() {
        return targetName;
    }

    public String dnsZone() {
        return dnsZone;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public String toString() {
        return "RotationData{" +
               "targetName='" + targetName + '\'' +
               ", zoneId=" + zoneId +
               ", dnsZone='" + dnsZone + '\'' +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RotationData that = (RotationData) o;
        return Objects.equals(targetName, that.targetName) &&
               Objects.equals(zoneId, that.zoneId) &&
               Objects.equals(dnsZone, that.dnsZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetName, zoneId, dnsZone);
    }
}
