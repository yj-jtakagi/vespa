// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "configpoller.h"
#include <vespa/config/common/exceptions.h>
#ifndef __linux__
#include <unistd.h>
#endif

#include <vespa/log/log.h>
LOG_SETUP(".config.helper.configpoller");

namespace config {

ConfigPoller::ConfigPoller(const IConfigContext::SP & context)
    : _generation(-1),
      _subscriber(context),
      _handleList(),
      _callbackList()
{
}

ConfigPoller::~ConfigPoller() = default;

void
ConfigPoller::run()
{
    try {
        while (!_subscriber.isClosed()) {
            poll();
        }
    } catch (config::InvalidConfigException & e) {
        LOG(fatal, "Got exception, will just exit quickly : %s", e.what());
#ifdef __linux__
        std::quick_exit(17);
#else
        _exit(17);
#endif
    }
}

void
ConfigPoller::poll()
{
    LOG(debug, "Checking for new config");
    if (_subscriber.nextGeneration()) {
        if (_subscriber.isClosed())
            return;
        LOG(debug, "Got new config, reconfiguring");
        _generation = _subscriber.getGeneration();
        for (size_t i = 0; i < _handleList.size(); i++) {
            ICallback * callback(_callbackList[i]);
            if (_handleList[i]->isChanged())
                callback->configure(_handleList[i]->getConfig());
        }
    } else {
        LOG(debug, "No new config available");
    }
}

void
ConfigPoller::close()
{
    _subscriber.close();
}

}
