// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "disk_mem_usage_sampler.h"
#include "document_db_explorer.h"
#include "fileconfigmanager.h"
#include "flushhandlerproxy.h"
#include "memoryflush.h"
#include "persistencehandlerproxy.h"
#include "prepare_restart_handler.h"
#include "proton.h"
#include "proton_config_snapshot.h"
#include "proton_disk_layout.h"
#include "resource_usage_explorer.h"
#include "searchhandlerproxy.h"
#include "simpleflush.h"

#include <vespa/searchcore/proton/flushengine/flushengine.h>
#include <vespa/searchcore/proton/flushengine/flush_engine_explorer.h>
#include <vespa/searchcore/proton/flushengine/prepare_restart_flush_strategy.h>
#include <vespa/searchcore/proton/flushengine/tls_stats_factory.h>
#include <vespa/searchcore/proton/reference/document_db_reference_registry.h>
#include <vespa/searchcore/proton/summaryengine/summaryengine.h>
#include <vespa/searchcore/proton/summaryengine/docsum_by_slime.h>
#include <vespa/searchcore/proton/matchengine/matchengine.h>
#include <vespa/searchlib/transactionlog/trans_log_server_explorer.h>
#include <vespa/searchlib/util/fileheadertk.h>
#include <vespa/document/base/exceptions.h>
#include <vespa/document/datatype/documenttype.h>
#include <vespa/document/repo/documenttyperepo.h>
#include <vespa/vespalib/io/fileutil.h>
#include <vespa/vespalib/util/closuretask.h>
#include <vespa/vespalib/util/lambdatask.h>
#include <vespa/vespalib/util/host_name.h>
#include <vespa/vespalib/util/random.h>
#include <vespa/searchlib/engine/transportserver.h>
#include <vespa/vespalib/net/state_server.h>

#include <vespa/searchlib/aggregation/forcelink.hpp>
#include <vespa/searchlib/expression/forcelink.hpp>
#include <sstream>

#include <vespa/log/log.h>
LOG_SETUP(".proton.server.proton");

using document::DocumentTypeRepo;
using vespalib::FileHeader;
using vespalib::IllegalStateException;
using vespalib::Slime;
using vespalib::makeLambdaTask;
using vespalib::slime::ArrayInserter;
using vespalib::slime::Cursor;

using search::transactionlog::DomainStats;
using vespa::config::search::core::ProtonConfig;
using vespa::config::search::core::internal::InternalProtonType;
using vespalib::compression::CompressionConfig;

namespace proton {

namespace {

using search::fs4transport::FS4PersistentPacketStreamer;

CompressionConfig::Type
convert(InternalProtonType::Packetcompresstype type)
{
    switch (type) {
      case InternalProtonType::Packetcompresstype::LZ4: return CompressionConfig::LZ4;
      default: return CompressionConfig::LZ4;
    }
}

void
setBucketCheckSumType(const ProtonConfig & proton)
{
    switch (proton.bucketdb.checksumtype) {
    case InternalProtonType::Bucketdb::Checksumtype::LEGACY:
        bucketdb::BucketState::setChecksumType(bucketdb::BucketState::ChecksumType::LEGACY);
        break;
    case InternalProtonType::Bucketdb::Checksumtype::XXHASH64:
        bucketdb::BucketState::setChecksumType(bucketdb::BucketState::ChecksumType::XXHASH64);
        break;
    }
}

void
setFS4Compression(const ProtonConfig & proton)
{
    FS4PersistentPacketStreamer & fs4(FS4PersistentPacketStreamer::Instance);
    fs4.SetCompressionLimit(proton.packetcompresslimit);
    fs4.SetCompressionLevel(proton.packetcompresslevel);
    fs4.SetCompressionType(convert(proton.packetcompresstype));
}

DiskMemUsageSampler::Config
diskMemUsageSamplerConfig(const ProtonConfig &proton, const HwInfo &hwInfo)
{
    return DiskMemUsageSampler::Config(
            proton.writefilter.memorylimit,
            proton.writefilter.disklimit,
            proton.writefilter.sampleinterval,
            hwInfo);
}

size_t
deriveCompactionCompressionThreads(const ProtonConfig &proton,
                                   const HwInfo::Cpu &cpuInfo) {
    size_t scaledCores = (size_t)std::ceil(cpuInfo.cores() * proton.feeding.concurrency);

    // We need at least 1 guaranteed free worker in order to ensure progress so #documentsdbs + 1 should suffice,
    // but we will not be cheap and give it one extra.
    return std::max(scaledCores, proton.documentdb.size() + proton.flush.maxconcurrent + 1);
}

const vespalib::string CUSTOM_COMPONENT_API_PATH = "/state/v1/custom/component";

VESPA_THREAD_STACK_TAG(proton_shared_executor)
VESPA_THREAD_STACK_TAG(index_warmup_executor)
VESPA_THREAD_STACK_TAG(initialize_executor)
VESPA_THREAD_STACK_TAG(close_executor)

}

Proton::ProtonFileHeaderContext::ProtonFileHeaderContext([[maybe_unused]] const Proton &proton_, const vespalib::string &creator)
    : _hostName(),
      _creator(creator),
      _cluster(),
      _pid(getpid())
{
    _hostName = vespalib::HostName::get();
    assert(!_hostName.empty());
}

Proton::ProtonFileHeaderContext::~ProtonFileHeaderContext() = default;

void
Proton::ProtonFileHeaderContext::addTags(vespalib::GenericHeader &header,
        const vespalib::string &name) const
{
    typedef vespalib::GenericHeader::Tag Tag;

    search::FileHeaderTk::addVersionTags(header);
    header.putTag(Tag("fileName", name));
    addCreateAndFreezeTime(header);
    header.putTag(Tag("hostName", _hostName));
    header.putTag(Tag("pid", _pid));
    header.putTag(Tag("creator", _creator));
    if (!_cluster.empty()) {
        header.putTag(Tag("cluster", _cluster));
    }
}


void
Proton::ProtonFileHeaderContext::setClusterName(const vespalib::string & clusterName,
                                                const vespalib::string & baseDir)
{
    if (!clusterName.empty()) {
        _cluster = clusterName;
        return;
    }
    // Derive cluster name from base dir.
    size_t cpos(baseDir.rfind('/'));
    if (cpos == vespalib::string::npos)
        return;
    size_t rpos(baseDir.rfind('/', cpos - 1));
    if (rpos == vespalib::string::npos)
        return;
    size_t clpos(baseDir.rfind('/', rpos - 1));
    if (clpos == vespalib::string::npos)
        return;
    if (baseDir.substr(clpos + 1, 8) != "cluster.")
        return;
    _cluster = baseDir.substr(clpos + 9, rpos - clpos - 9);
}


Proton::Proton(const config::ConfigUri & configUri,
               const vespalib::string &progName,
               uint64_t subscribeTimeout)
    : IProtonConfigurerOwner(),
      search::engine::MonitorServer(),
      IDocumentDBOwner(),
      StatusProducer(),
      IPersistenceEngineOwner(),
      ComponentConfigProducer(),
      _configUri(configUri),
      _mutex(),
      _metricsHook(*this),
      _metricsEngine(std::make_unique<MetricsEngine>()),
      _fileHeaderContext(*this, progName),
      _tls(),
      _diskMemUsageSampler(),
      _persistenceEngine(),
      _documentDBMap(),
      _matchEngine(),
      _summaryEngine(),
      _docsumBySlime(),
      _memoryFlushConfigUpdater(),
      _flushEngine(),
      _prepareRestartHandler(),
      _rpcHooks(),
      _healthAdapter(*this),
      _genericStateHandler(CUSTOM_COMPONENT_API_PATH, *this),
      _customComponentBindToken(),
      _customComponentRootToken(),
      _stateServer(),
      _fs4Server(),
      // This executor can only have 1 thread as it is used for
      // serializing startup.
      _executor(1, 128 * 1024),
      _protonDiskLayout(),
      _protonConfigurer(_executor, *this, _protonDiskLayout),
      _protonConfigFetcher(configUri, _protonConfigurer, subscribeTimeout),
      _warmupExecutor(),
      _sharedExecutor(),
      _queryLimiter(),
      _clock(0.010),
      _threadPool(128 * 1024),
      _distributionKey(-1),
      _isInitializing(true),
      _isReplayDone(false),
      _abortInit(false),
      _initStarted(false),
      _initComplete(false),
      _initDocumentDbsInSequence(false),
      _documentDBReferenceRegistry(),
      _nodeUpLock(),
      _nodeUp()
{
    _documentDBReferenceRegistry = std::make_shared<DocumentDBReferenceRegistry>();
}

BootstrapConfig::SP
Proton::init()
{
    assert( ! _initStarted && ! _initComplete );
    _initStarted = true;
    if (_threadPool.NewThread(&_clock, nullptr) == nullptr) {
        throw IllegalStateException("Failed starting thread for the cheap clock");
    }
    _protonConfigFetcher.start();
    auto configSnapshot = _protonConfigurer.getPendingConfigSnapshot();
    assert(configSnapshot);
    auto bootstrapConfig = configSnapshot->getBootstrapConfig();
    assert(bootstrapConfig);

    return bootstrapConfig;
}

void
Proton::init(const BootstrapConfig::SP & configSnapshot)
{
    assert( _initStarted && ! _initComplete );
    const ProtonConfig &protonConfig = configSnapshot->getProtonConfig();
    const HwInfo & hwInfo = configSnapshot->getHwInfo();

    setBucketCheckSumType(protonConfig);
    setFS4Compression(protonConfig);
    _diskMemUsageSampler = std::make_unique<DiskMemUsageSampler>(protonConfig.basedir,
                                                                 diskMemUsageSamplerConfig(protonConfig, hwInfo));

    _tls = std::make_unique<TLS>(_configUri.createWithNewId(protonConfig.tlsconfigid), _fileHeaderContext);
    _metricsEngine->addMetricsHook(_metricsHook);
    _fileHeaderContext.setClusterName(protonConfig.clustername, protonConfig.basedir);
    _matchEngine = std::make_unique<MatchEngine>(protonConfig.numsearcherthreads,
                                                 protonConfig.numthreadspersearch,
                                                 protonConfig.distributionkey);
    _distributionKey = protonConfig.distributionkey;
    _summaryEngine= std::make_unique<SummaryEngine>(protonConfig.numsummarythreads);
    _docsumBySlime = std::make_unique<DocsumBySlime>(*_summaryEngine);

    IFlushStrategy::SP strategy;
    const ProtonConfig::Flush & flush(protonConfig.flush);
    switch (flush.strategy) {
    case ProtonConfig::Flush::Strategy::MEMORY: {
        auto memoryFlush = std::make_shared<MemoryFlush>(
                MemoryFlushConfigUpdater::convertConfig(flush.memory, hwInfo.memory()), fastos::ClockSystem::now());
        _memoryFlushConfigUpdater = std::make_unique<MemoryFlushConfigUpdater>(memoryFlush, flush.memory, hwInfo.memory());
        _diskMemUsageSampler->notifier().addDiskMemUsageListener(_memoryFlushConfigUpdater.get());
        strategy = memoryFlush;
        break;
    }
    case ProtonConfig::Flush::Strategy::SIMPLE:
    default:
        strategy = std::make_shared<SimpleFlush>();
        break;
    }
    _protonDiskLayout = std::make_unique<ProtonDiskLayout>(protonConfig.basedir, protonConfig.tlsspec);
    vespalib::chdir(protonConfig.basedir);
    _tls->start();
    _flushEngine = std::make_unique<FlushEngine>(std::make_shared<flushengine::TlsStatsFactory>(_tls->getTransLogServer()),
                                                 strategy, flush.maxconcurrent, flush.idleinterval*1000);
    _fs4Server = std::make_unique<TransportServer>(*_matchEngine, *_summaryEngine, *this, protonConfig.ptport, TransportServer::DEBUG_ALL);
    _fs4Server->setTCPNoDelay(true);
    _metricsEngine->addExternalMetrics(_fs4Server->getMetrics());
    _metricsEngine->addExternalMetrics(_summaryEngine->getMetrics());

    char tmp[1024];
    LOG(debug, "Start proton server with root at %s and cwd at %s",
        protonConfig.basedir.c_str(), getcwd(tmp, sizeof(tmp)));

    _persistenceEngine = std::make_unique<PersistenceEngine>(*this, _diskMemUsageSampler->writeFilter(),
                                                             protonConfig.visit.defaultserializedsize,
                                                             protonConfig.visit.ignoremaxbytes);

    vespalib::string fileConfigId;
    _warmupExecutor = std::make_unique<vespalib::ThreadStackExecutor>(4, 128*1024, index_warmup_executor);

    const size_t sharedThreads = deriveCompactionCompressionThreads(protonConfig, hwInfo.cpu());
    _sharedExecutor = std::make_unique<vespalib::BlockingThreadStackExecutor>(sharedThreads, 128*1024, sharedThreads*16, proton_shared_executor);
    InitializeThreads initializeThreads;
    if (protonConfig.initialize.threads > 0) {
        initializeThreads = std::make_shared<vespalib::ThreadStackExecutor>(protonConfig.initialize.threads, 128 * 1024, initialize_executor);
        _initDocumentDbsInSequence = (protonConfig.initialize.threads == 1);
    }
    _protonConfigurer.applyInitialConfig(initializeThreads);
    initializeThreads.reset();

    _prepareRestartHandler = std::make_unique<PrepareRestartHandler>(*_flushEngine);
    RPCHooks::Params rpcParams(*this, protonConfig.rpcport, _configUri.getConfigId());
    rpcParams.slobrok_config = _configUri.createWithNewId(protonConfig.slobrokconfigid);
    _rpcHooks = std::make_unique<RPCHooks>(rpcParams);

    waitForInitDone();

    _metricsEngine->start(_configUri);
    _stateServer = std::make_unique<vespalib::StateServer>(protonConfig.httpport, _healthAdapter,
                                                           _metricsEngine->metrics_producer(), *this);
    _customComponentBindToken = _stateServer->repo().bind(CUSTOM_COMPONENT_API_PATH, _genericStateHandler);
    _customComponentRootToken = _stateServer->repo().add_root_resource(CUSTOM_COMPONENT_API_PATH);

    _executor.sync();
    waitForOnlineState();
    _isReplayDone = true;
    _rpcHooks->set_online();
    if ( ! _fs4Server->start() ) {
        throw vespalib::PortListenException(protonConfig.ptport, "FS4");
    }
    int port = _fs4Server->getListenPort();
    LOG(debug, "Started fs4 interface on port %d", port);
    _flushEngine->start();
    _isInitializing = false;
    _protonConfigurer.setAllowReconfig(true);
    _initComplete = true;
}

BootstrapConfig::SP
Proton::getActiveConfigSnapshot() const
{
    return _protonConfigurer.getActiveConfigSnapshot()->getBootstrapConfig();
}

void
Proton::applyConfig(const BootstrapConfig::SP & configSnapshot)
{
    // Called by executor thread during reconfig.
    const ProtonConfig &protonConfig = configSnapshot->getProtonConfig();
    setFS4Compression(protonConfig);

    _queryLimiter.configure(protonConfig.search.memory.limiter.maxthreads,
                            protonConfig.search.memory.limiter.mincoverage,
                            protonConfig.search.memory.limiter.minhits);
    const std::shared_ptr<const DocumentTypeRepo> repo = configSnapshot->getDocumentTypeRepoSP();

    _diskMemUsageSampler->setConfig(diskMemUsageSamplerConfig(protonConfig, configSnapshot->getHwInfo()));
    if (_memoryFlushConfigUpdater) {
        _memoryFlushConfigUpdater->setConfig(protonConfig.flush.memory);
        _flushEngine->kick();
    }
}

std::shared_ptr<DocumentDBConfigOwner>
Proton::addDocumentDB(const DocTypeName &docTypeName,
                      document::BucketSpace bucketSpace,
                      const vespalib::string &configId,
                      const BootstrapConfig::SP &bootstrapConfig,
                      const DocumentDBConfig::SP &documentDBConfig,
                      InitializeThreads initializeThreads)
{
    try {
        const std::shared_ptr<const DocumentTypeRepo> repo = bootstrapConfig->getDocumentTypeRepoSP();
        const document::DocumentType *docType = repo->getDocumentType(docTypeName.getName());
        if (docType != NULL) {
            LOG(info, "Add document database: doctypename(%s), configid(%s)",
                docTypeName.toString().c_str(), configId.c_str());
            return addDocumentDB(*docType, bucketSpace, bootstrapConfig, documentDBConfig, initializeThreads);
        } else {

            LOG(warning,
                "Did not find document type '%s' in the document manager. "
                "Skipping creating document database for this type",
                docTypeName.toString().c_str());
            return std::shared_ptr<DocumentDBConfigOwner>();
        }
    } catch (const document::DocumentTypeNotFoundException & e) {
        LOG(warning,
            "Did not find document type '%s' in the document manager. "
            "Skipping creating document database for this type",
            docTypeName.toString().c_str());
        return std::shared_ptr<DocumentDBConfigOwner>();
    }
}

Proton::~Proton()
{
    assert(_initStarted);
    if ( ! _initComplete ) {
        LOG(warning, "Initialization of proton was halted. Shutdown sequence has been initiated.");
    }
    _protonConfigFetcher.close();
    _protonConfigurer.setAllowReconfig(false);
    _executor.sync();
    _customComponentRootToken.reset();
    _customComponentBindToken.reset();
    _stateServer.reset();
    if (_metricsEngine) {
        _metricsEngine->removeMetricsHook(_metricsHook);
        _metricsEngine->stop();
    }
    if (_matchEngine) {
        _matchEngine->close();
    }
    if (_metricsEngine && _summaryEngine) {
        _metricsEngine->removeExternalMetrics(_summaryEngine->getMetrics());
    }
    if (_summaryEngine) {
        _summaryEngine->close();
    }
    if (_rpcHooks) {
        _rpcHooks->close();
    }
    if (_memoryFlushConfigUpdater) {
        _diskMemUsageSampler->notifier().removeDiskMemUsageListener(_memoryFlushConfigUpdater.get());
    }
    _executor.shutdown();
    _executor.sync();
    _rpcHooks.reset();
    if (_flushEngine) {
        _flushEngine->close();
    }
    if (_warmupExecutor) {
        _warmupExecutor->sync();
    }
    if (_sharedExecutor) {
        _sharedExecutor->sync();
    }
    LOG(debug, "Shutting down fs4 interface");
    if (_metricsEngine && _fs4Server) {
        _metricsEngine->removeExternalMetrics(_fs4Server->getMetrics());
    }
    if (_fs4Server) {
        _fs4Server->shutDown();
    }
    if (_documentDBMap.size() > 0) {
        size_t numCores = 4;
        const std::shared_ptr<proton::ProtonConfigSnapshot> pcsp = _protonConfigurer.getActiveConfigSnapshot();
        if (pcsp) {
            const std::shared_ptr<proton::BootstrapConfig> bcp = pcsp->getBootstrapConfig();
            if (bcp) {
                numCores = std::max(bcp->getHwInfo().cpu().cores(), 1u);
            }
        }

        vespalib::ThreadStackExecutor closePool(std::min(_documentDBMap.size(), numCores), 0x20000, close_executor);
        closeDocumentDBs(closePool);
    }
    _documentDBMap.clear();
    _persistenceEngine.reset();
    _tls.reset();
    _warmupExecutor.reset();
    _sharedExecutor.reset();
    _clock.stop();
    LOG(debug, "Explicit destructor done");
}

void
Proton::closeDocumentDBs(vespalib::ThreadStackExecutorBase & executor) {
    // Need to extract names first as _documentDBMap is modified while removing.
    std::vector<DocTypeName> docTypes;
    docTypes.reserve(_documentDBMap.size());
    for (const auto & entry : _documentDBMap) {
        docTypes.push_back(entry.first);
    }
    for (const auto & docTypeName : docTypes) {
        executor.execute(makeLambdaTask([this, docTypeName]() { removeDocumentDB(docTypeName); }));
    }
    executor.sync();
}

size_t Proton::getNumDocs() const
{
    size_t numDocs(0);
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    for (const auto &kv : _documentDBMap) {
        numDocs += kv.second->getNumDocs();
    }
    return numDocs;
}

size_t Proton::getNumActiveDocs() const
{
    size_t numDocs(0);
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    for (const auto &kv : _documentDBMap) {
        numDocs += kv.second->getNumActiveDocs();
    }
    return numDocs;
}

search::engine::SearchServer &
Proton::get_search_server()
{
    return *_matchEngine;
}

search::engine::DocsumServer &
Proton::get_docsum_server()
{
    return *_summaryEngine;
}

search::engine::MonitorServer &
Proton::get_monitor_server()
{
    return *this;
}

vespalib::string
Proton::getDelayedConfigs() const
{
    std::ostringstream res;
    bool first = true;
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    for (const auto &kv : _documentDBMap) {
        if (kv.second->getDelayedConfig()) {
            if (!first) {
                res << ", ";
            }
            first = false;
            res << kv.first.toString();
        }
    }
    return res.str();
}

StatusReport::List
Proton::getStatusReports() const
{
    StatusReport::List reports;
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    reports.push_back(StatusReport::SP(_matchEngine->reportStatus()));
    for (const auto &kv : _documentDBMap) {
        reports.push_back(StatusReport::SP(kv.second->reportStatus()));
    }
    return reports;
}

DocumentDB::SP
Proton::addDocumentDB(const document::DocumentType &docType,
                      document::BucketSpace bucketSpace,
                      const BootstrapConfig::SP &bootstrapConfig,
                      const DocumentDBConfig::SP &documentDBConfig,
                      InitializeThreads initializeThreads)
{
    const ProtonConfig &config(bootstrapConfig->getProtonConfig());

    std::lock_guard<std::shared_timed_mutex> guard(_mutex);
    DocTypeName docTypeName(docType.getName());
    DocumentDBMap::iterator it = _documentDBMap.find(docTypeName);
    if (it != _documentDBMap.end()) {
        return it->second;
    }

    vespalib::string db_dir = config.basedir + "/documents/" + docTypeName.toString();
    vespalib::mkdir(db_dir, false); // Assume parent is created.
    auto config_store = std::make_unique<FileConfigManager>(db_dir + "/config",
                                                            documentDBConfig->getConfigId(),
                                                            docTypeName.getName());
    config_store->setProtonConfig(bootstrapConfig->getProtonConfigSP());
    if (!initializeThreads) {
        // If configured value for initialize threads was 0, or we
        // are performing a reconfig after startup has completed, then use
        // 1 thread per document type.
        initializeThreads = std::make_shared<vespalib::ThreadStackExecutor>(1, 128 * 1024);
    }
    auto ret = std::make_shared<DocumentDB>(config.basedir + "/documents", documentDBConfig, config.tlsspec,
                                            _queryLimiter, _clock, docTypeName, bucketSpace, config, *this,
                                            *_warmupExecutor, *_sharedExecutor, *_tls->getTransLogServer(),
                                            *_metricsEngine, _fileHeaderContext, std::move(config_store),
                                            initializeThreads, bootstrapConfig->getHwInfo());
    try {
        ret->start();
    } catch (vespalib::Exception &e) {
        LOG(warning, "Failed to start database for document type '%s'; %s",
            docTypeName.toString().c_str(), e.what());
        return DocumentDB::SP();
    }
    // Wait for replay done on document dbs added due to reconfigs, since engines are already up and running.
    // Also wait for document db reaching online state if initializing in sequence.
    if (!_isInitializing || _initDocumentDbsInSequence) {
        ret->waitForOnlineState();
    }
    _metricsEngine->addDocumentDBMetrics(ret->getMetrics());
    _metricsEngine->addMetricsHook(ret->getMetricsUpdateHook());
    _documentDBMap[docTypeName] = ret;
    if (_persistenceEngine) {
        // Not allowed to get to service layer to call pause().
        std::unique_lock<std::shared_timed_mutex> persistenceWGuard(_persistenceEngine->getWLock());
        auto persistenceHandler = std::make_shared<PersistenceHandlerProxy>(ret);
        if (!_isInitializing) {
            _persistenceEngine->propagateSavedClusterState(bucketSpace, *persistenceHandler);
            _persistenceEngine->populateInitialBucketDB(bucketSpace, *persistenceHandler);
        }
        // TODO: Fix race with new cluster state setting.
        _persistenceEngine->putHandler(bucketSpace, docTypeName, persistenceHandler);
    }
    auto searchHandler = std::make_shared<SearchHandlerProxy>(ret);
    _summaryEngine->putSearchHandler(docTypeName, searchHandler);
    _matchEngine->putSearchHandler(docTypeName, searchHandler);
    auto flushHandler = std::make_shared<FlushHandlerProxy>(ret);
    _flushEngine->putFlushHandler(docTypeName, flushHandler);
    _diskMemUsageSampler->notifier().addDiskMemUsageListener(ret->diskMemUsageListener());
    return ret;
}


void
Proton::removeDocumentDB(const DocTypeName &docTypeName)
{
    DocumentDB::SP old;
    {
        std::lock_guard<std::shared_timed_mutex> guard(_mutex);
        DocumentDBMap::iterator it = _documentDBMap.find(docTypeName);
        if (it == _documentDBMap.end()) {
            return;
        }
        old = it->second;
        _documentDBMap.erase(it);
    }

    // Remove all entries into document db
    if (_persistenceEngine) {
        {
            // Not allowed to get to service layer to call pause().
            std::unique_lock<std::shared_timed_mutex> persistenceWguard(_persistenceEngine->getWLock());
            IPersistenceHandler::SP oldHandler;
            oldHandler = _persistenceEngine->removeHandler(old->getBucketSpace(), docTypeName);
            if (_initComplete && oldHandler) {
                // TODO: Fix race with bucket db modifying ops.
                _persistenceEngine->grabExtraModifiedBuckets(old->getBucketSpace(), *oldHandler);
            }
        }
        _persistenceEngine->destroyIterators();
    }
    _matchEngine->removeSearchHandler(docTypeName);
    _summaryEngine->removeSearchHandler(docTypeName);
    _flushEngine->removeFlushHandler(docTypeName);
    _metricsEngine->removeMetricsHook(old->getMetricsUpdateHook());
    _metricsEngine->removeDocumentDBMetrics(old->getMetrics());
    _diskMemUsageSampler->notifier().removeDiskMemUsageListener(old->diskMemUsageListener());
    // Caller should have removed & drained relevant timer tasks
    old->close();
}


Proton::MonitorReply::UP
Proton::ping(MonitorRequest::UP request, MonitorClient & client)
{
    (void) client;
    auto reply = std::make_unique<MonitorReply>();
    MonitorReply &ret = *reply;

    BootstrapConfig::SP configSnapshot = getActiveConfigSnapshot();
    const ProtonConfig &protonConfig = configSnapshot->getProtonConfig();
    ret.partid = protonConfig.partition;
    ret.distribution_key = protonConfig.distributionkey;
    ret.timestamp = (_matchEngine->isOnline()) ? 42 : 0;
    ret.activeDocs = (_matchEngine->isOnline()) ? getNumActiveDocs() : 0;
    ret.activeDocsRequested = request->reportActiveDocs;
    return reply;
}

bool
Proton::triggerFlush()
{
    if (!_flushEngine || ! _flushEngine->HasThread()) {
        return false;
    }
    _flushEngine->triggerFlush();
    return true;
}

bool
Proton::prepareRestart()
{
    BootstrapConfig::SP configSnapshot = getActiveConfigSnapshot();
    return _prepareRestartHandler->prepareRestart(configSnapshot->getProtonConfig());
}

namespace {

int countOpenFiles()
{
#ifdef __linux__
    static const char * const fd_dir_name = "/proc/self/fd";
    int count = 0;
    DIR *dp = opendir(fd_dir_name);
    if (dp != nullptr) {
        struct dirent *ptr;
        while ((ptr = readdir(dp)) != nullptr) {
            if (strcmp(".", ptr->d_name) == 0) continue;
            if (strcmp("..", ptr->d_name) == 0) continue;
            ++count;
        }
        closedir(dp);
    } else {
        LOG(warning, "could not scan directory %s: %s", fd_dir_name, strerror(errno));
    }
    return count;
#else
    return 0;
#endif
}

void
updateExecutorMetrics(ExecutorMetrics &metrics,
                      const vespalib::ThreadStackExecutor::Stats &stats)
{
    metrics.update(stats);
}

}

void
Proton::updateMetrics(const vespalib::MonitorGuard &)
{
    {
        ContentProtonMetrics &metrics = _metricsEngine->root();
        auto tls = _tls->getTransLogServer();
        if (tls) {
            metrics.transactionLog.update(tls->getDomainStats());
        }

        const DiskMemUsageFilter &usageFilter = _diskMemUsageSampler->writeFilter();
        DiskMemUsageState usageState = usageFilter.usageState();
        metrics.resourceUsage.disk.set(usageState.diskState().usage());
        metrics.resourceUsage.diskUtilization.set(usageState.diskState().utilization());
        metrics.resourceUsage.memory.set(usageState.memoryState().usage());
        metrics.resourceUsage.memoryUtilization.set(usageState.memoryState().utilization());
        metrics.resourceUsage.memoryMappings.set(usageFilter.getMemoryStats().getMappingsCount());
        metrics.resourceUsage.openFileDescriptors.set(countOpenFiles());
        metrics.resourceUsage.feedingBlocked.set((usageFilter.acceptWriteOperation() ? 0.0 : 1.0));
    }
    {
        ContentProtonMetrics::ProtonExecutorMetrics &metrics = _metricsEngine->root().executor;
        updateExecutorMetrics(metrics.proton, _executor.getStats());
        if (_flushEngine) {
            updateExecutorMetrics(metrics.flush, _flushEngine->getExecutorStats());
        }
        if (_matchEngine) {
            updateExecutorMetrics(metrics.match, _matchEngine->getExecutorStats());
        }
        if (_summaryEngine) {
            updateExecutorMetrics(metrics.docsum, _summaryEngine->getExecutorStats());
        }
        if (_sharedExecutor) {
            metrics.shared.update(_sharedExecutor->getStats());
        }
        if (_warmupExecutor) {
            metrics.warmup.update(_warmupExecutor->getStats());
        }
    }
}

void
Proton::waitForInitDone()
{
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    for (const auto &kv : _documentDBMap) {
        kv.second->waitForInitDone();
    }
}

void
Proton::waitForOnlineState()
{
    std::shared_lock<std::shared_timed_mutex> guard(_mutex);
    for (const auto &kv : _documentDBMap) {
        kv.second->waitForOnlineState();
    }
}

void
Proton::getComponentConfig(Consumer &consumer)
{
    _protonConfigurer.getComponentConfig().getComponentConfig(consumer);
    std::vector<DocumentDB::SP> dbs;
    {
        std::shared_lock<std::shared_timed_mutex> guard(_mutex);
        for (const auto &kv : _documentDBMap) {
            dbs.push_back(kv.second);
        }
    }
    for (const auto &docDb : dbs) {
        vespalib::string name("proton.documentdb.");
        name.append(docDb->getDocTypeName().getName());
        int64_t gen = docDb->getActiveGeneration();
        if (docDb->getDelayedConfig()) {
            consumer.add(Config(name, gen, "has delayed attribute aspect change in config"));
        } else {
            consumer.add(Config(name, gen));
        }
    }
}

int64_t
Proton::getConfigGeneration()
{
    return _protonConfigurer.getActiveConfigSnapshot()->getBootstrapConfig()->getGeneration();
}

bool
Proton::updateNodeUp(BucketSpace bucketSpace, bool nodeUpInBucketSpace)
{
    std::lock_guard guard(_nodeUpLock);
    if (nodeUpInBucketSpace) {
        _nodeUp.insert(bucketSpace);
    } else {
        _nodeUp.erase(bucketSpace);
    }
    return !_nodeUp.empty();
}

void
Proton::setClusterState(BucketSpace bucketSpace, const storage::spi::ClusterState &calc)
{
    // forward info sent by cluster controller to persistence engine
    // about whether node is supposed to be up or not.  Match engine
    // needs to know this in order to stop serving queries.
    bool nodeUpInBucketSpace(calc.nodeUp());
    bool nodeRetired(calc.nodeRetired());
    bool nodeUp = updateNodeUp(bucketSpace, nodeUpInBucketSpace);
    _matchEngine->setNodeUp(nodeUp);
    if (_memoryFlushConfigUpdater) {
        _memoryFlushConfigUpdater->setNodeRetired(nodeRetired);
    }
}

namespace {

const vespalib::string MATCH_ENGINE = "matchengine";
const vespalib::string DOCUMENT_DB = "documentdb";
const vespalib::string FLUSH_ENGINE = "flushengine";
const vespalib::string TLS_NAME = "tls";
const vespalib::string RESOURCE_USAGE = "resourceusage";

struct StateExplorerProxy : vespalib::StateExplorer {
    const StateExplorer &explorer;
    explicit StateExplorerProxy(const StateExplorer &explorer_in) : explorer(explorer_in) {}
    virtual void get_state(const vespalib::slime::Inserter &inserter, bool full) const override { explorer.get_state(inserter, full); }
    virtual std::vector<vespalib::string> get_children_names() const override { return explorer.get_children_names(); }
    virtual std::unique_ptr<vespalib::StateExplorer> get_child(vespalib::stringref name) const override { return explorer.get_child(name); }
};

struct DocumentDBMapExplorer : vespalib::StateExplorer {
    typedef std::map<DocTypeName, DocumentDB::SP> DocumentDBMap;
    const DocumentDBMap &documentDBMap;
    std::shared_timed_mutex &mutex;
    DocumentDBMapExplorer(const DocumentDBMap &documentDBMap_in, std::shared_timed_mutex &mutex_in)
        : documentDBMap(documentDBMap_in), mutex(mutex_in) {}
    void get_state(const vespalib::slime::Inserter &, bool) const override {}
    std::vector<vespalib::string> get_children_names() const override {
        std::shared_lock<std::shared_timed_mutex> guard(mutex);
        std::vector<vespalib::string> names;
        for (const auto &item: documentDBMap) {
            names.push_back(item.first.getName());
        }
        return names;
    }
    std::unique_ptr<vespalib::StateExplorer> get_child(vespalib::stringref name) const override {
        typedef std::unique_ptr<StateExplorer> Explorer_UP;
        std::shared_lock<std::shared_timed_mutex> guard(mutex);
        auto result = documentDBMap.find(DocTypeName(vespalib::string(name)));
        if (result == documentDBMap.end()) {
            return Explorer_UP(nullptr);
        }
        return std::make_unique<DocumentDBExplorer>(result->second);
    }
};

} // namespace proton::<unnamed>

void
Proton::get_state(const vespalib::slime::Inserter &, bool) const
{
}

std::vector<vespalib::string>
Proton::get_children_names() const
{
    std::vector<vespalib::string> names({DOCUMENT_DB, MATCH_ENGINE, FLUSH_ENGINE, TLS_NAME, RESOURCE_USAGE});
    return names;
}

std::unique_ptr<vespalib::StateExplorer>
Proton::get_child(vespalib::stringref name) const
{
    typedef std::unique_ptr<StateExplorer> Explorer_UP;
    if (name == MATCH_ENGINE && _matchEngine) {
        return std::make_unique<StateExplorerProxy>(*_matchEngine);
    } else if (name == DOCUMENT_DB) {
        return std::make_unique<DocumentDBMapExplorer>(_documentDBMap, _mutex);
    } else if (name == FLUSH_ENGINE && _flushEngine) {
        return std::make_unique<FlushEngineExplorer>(*_flushEngine);
    } else if (name == TLS_NAME && _tls) {
        return std::make_unique<search::transactionlog::TransLogServerExplorer>(_tls->getTransLogServer());
    } else if (name == RESOURCE_USAGE && _diskMemUsageSampler) {
        return std::make_unique<ResourceUsageExplorer>(_diskMemUsageSampler->writeFilter());
    }
    return Explorer_UP(nullptr);
}

std::shared_ptr<IDocumentDBReferenceRegistry>
Proton::getDocumentDBReferenceRegistry() const
{
    return _documentDBReferenceRegistry;
}

} // namespace proton
