// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "disk_mem_usage_filter.h"

namespace vespalib { class Timer; }

namespace proton {

/*
 * Class to sample disk and memory usage used for filtering write operations.
 */
class DiskMemUsageSampler {
    DiskMemUsageFilter _filter;
public:
#if defined(__APPLE__) && defined(__clang__)
    using fs_path_type = std::string;
#else
    using fs_path_type = std::filesystem::path;
#endif
private:
    fs_path_type _path;
    double _sampleInterval;
    std::unique_ptr<vespalib::Timer> _periodicTimer;

    void sampleUsage();
    void sampleDiskUsage();
    void sampleMemoryUsage();
public:
    struct Config {
        DiskMemUsageFilter::Config filterConfig;
        double sampleInterval;
        HwInfo hwInfo;

        Config()
            : filterConfig(),
              sampleInterval(60.0),
              hwInfo()
        {
        }

        Config(double memoryLimit_in,
               double diskLimit_in,
               double sampleInterval_in,
               const HwInfo &hwInfo_in)
            : filterConfig(memoryLimit_in, diskLimit_in),
              sampleInterval(sampleInterval_in),
              hwInfo(hwInfo_in)
        {
        }
    };

    DiskMemUsageSampler(const std::string &path_in,
                        const Config &config);

    ~DiskMemUsageSampler();

    void setConfig(const Config &config);

    const DiskMemUsageFilter &writeFilter() const { return _filter; }
    IDiskMemUsageNotifier &notifier() { return _filter; }
};


} // namespace proton
