// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "combiningfeedview.h"
#include "commit_and_wait_document_retriever.h"
#include "document_subdb_collection_initializer.h"
#include "documentsubdbcollection.h"
#include "i_document_subdb_owner.h"
#include "maintenancecontroller.h"
#include "searchabledocsubdb.h"
#include <vespa/searchcore/proton/metrics/documentdb_tagged_metrics.h>
#include <vespa/vespalib/util/lambdatask.h>

using proton::matching::SessionManager;
using search::GrowStrategy;
using search::SerialNum;
using search::index::Schema;
using searchcorespi::IFlushTarget;
using vespalib::makeLambdaTask;

namespace proton {

DocumentSubDBCollection::Config::Config(GrowStrategy ready, GrowStrategy notReady, GrowStrategy removed,
                                        size_t fixedAttributeTotalSkew, size_t numSearchThreads)
    : _readyGrowth(ready),
      _notReadyGrowth(notReady),
      _removedGrowth(removed),
      _fixedAttributeTotalSkew(fixedAttributeTotalSkew),
      _numSearchThreads(numSearchThreads)
{ }

DocumentSubDBCollection::DocumentSubDBCollection(
        IDocumentSubDBOwner &owner,
        search::transactionlog::SyncProxy &tlSyncer,
        const IGetSerialNum &getSerialNum,
        const DocTypeName &docTypeName,
        searchcorespi::index::IThreadingService &writeService,
        vespalib::ThreadExecutor &warmupExecutor,
        const search::common::FileHeaderContext &fileHeaderContext,
        MetricsWireService &metricsWireService,
        DocumentDBTaggedMetrics &metrics,
        matching::QueryLimiter &queryLimiter,
        const vespalib::Clock &clock,
        std::mutex &configMutex,
        const vespalib::string &baseDir,
        const Config & cfg,
        const HwInfo &hwInfo)
    : _subDBs(),
      _owner(owner),
      _calc(),
      _readySubDbId(0),
      _remSubDbId(1),
      _notReadySubDbId(2),
      _retrievers(),
      _reprocessingRunner(),
      _bucketDB(),
      _bucketDBHandler(),
      _hwInfo(hwInfo)
{
    _bucketDB = std::make_shared<BucketDBOwner>();
    _bucketDBHandler = std::make_unique<bucketdb::BucketDBHandler>(*_bucketDB);

    StoreOnlyDocSubDB::Context context(owner, tlSyncer, getSerialNum, fileHeaderContext, writeService,
                                       _bucketDB, *_bucketDBHandler, metrics, configMutex, hwInfo);
    _subDBs.push_back
        (new SearchableDocSubDB(
                SearchableDocSubDB::Config(
                    FastAccessDocSubDB::Config(
                            StoreOnlyDocSubDB::Config(docTypeName, "0.ready", baseDir,
                                    cfg.getReadyGrowth(), cfg.getFixedAttributeTotalSkew(),
                                    _readySubDbId, SubDbType::READY),
                            true, true, false),
                    cfg.getNumSearchThreads()),
                SearchableDocSubDB::Context(
                        FastAccessDocSubDB::Context(context, metrics.ready.attributes, metricsWireService),
                        queryLimiter, clock, warmupExecutor)));

    _subDBs.push_back
        (new StoreOnlyDocSubDB(
                StoreOnlyDocSubDB::Config(docTypeName, "1.removed", baseDir, cfg.getRemovedGrowth(),
                        cfg.getFixedAttributeTotalSkew(), _remSubDbId, SubDbType::REMOVED),
                context));

    _subDBs.push_back
        (new FastAccessDocSubDB(
                FastAccessDocSubDB::Config(
                        StoreOnlyDocSubDB::Config(docTypeName, "2.notready", baseDir,
                                cfg.getNotReadyGrowth(), cfg.getFixedAttributeTotalSkew(),
                                _notReadySubDbId, SubDbType::NOTREADY),
                        true, true, true),
                FastAccessDocSubDB::Context(context, metrics.notReady.attributes, metricsWireService)));
}


DocumentSubDBCollection::~DocumentSubDBCollection()
{
    size_t numThreads = std::min(_subDBs.size(), static_cast<size_t>(_hwInfo.cpu().cores()));
    vespalib::ThreadStackExecutor closePool(numThreads, 0x20000);
    while (!_subDBs.empty()) {
        closePool.execute(makeLambdaTask([subDB=_subDBs.back()]() { delete subDB; }));
        _subDBs.pop_back();
    }
    closePool.sync();

    _bucketDB.reset();

    RetrieversSP retrievers = _retrievers.get();
    _retrievers.clear();
    if (retrievers) {
        while (!retrievers->empty()) {
            auto retriever = std::move(retrievers->back());
            retrievers->pop_back();
            closePool.execute(makeLambdaTask([r = std::move(retriever)]() mutable { r.reset(); }));
        }
    }
    closePool.sync();
}

void
DocumentSubDBCollection::createRetrievers()
{
    RetrieversSP retrievers(std::make_shared<std::vector<IDocumentRetriever::SP>>());
    retrievers->reserve(_subDBs.size());
    for (auto subDb : _subDBs) {
        retrievers->emplace_back(subDb->getDocumentRetriever());
    }
    _retrievers.set(retrievers);
}

namespace {

IDocumentRetriever::SP
wrapRetriever(const IDocumentRetriever::SP &retriever, ICommitable &commit)
{
    return std::make_shared<CommitAndWaitDocumentRetriever>(retriever, commit);
}

}


void DocumentSubDBCollection::maintenanceSync(MaintenanceController &mc, ICommitable &commit) {
    RetrieversSP retrievers = getRetrievers();
    MaintenanceDocumentSubDB readySubDB(getReadySubDB()->getName(),
                                        _readySubDbId,
                                        getReadySubDB()->getDocumentMetaStoreContext().getSP(),
                                        wrapRetriever((*retrievers)[_readySubDbId], commit),
                                        getReadySubDB()->getFeedView());
    MaintenanceDocumentSubDB remSubDB(getRemSubDB()->getName(),
                                      _remSubDbId,
                                      getRemSubDB()->getDocumentMetaStoreContext().getSP(),
                                      (*retrievers)[_remSubDbId],
                                      getRemSubDB()->getFeedView());
    MaintenanceDocumentSubDB notReadySubDB(getNotReadySubDB()->getName(),
                                           _notReadySubDbId,
                                           getNotReadySubDB()->getDocumentMetaStoreContext().getSP(),
                                           wrapRetriever((*retrievers)[_notReadySubDbId], commit),
                                           getNotReadySubDB()->getFeedView());
    mc.syncSubDBs(readySubDB, remSubDB, notReadySubDB);
}

initializer::InitializerTask::SP
DocumentSubDBCollection::createInitializer(const DocumentDBConfig &configSnapshot,
                                           SerialNum configSerialNum,
                                           const index::IndexConfig & indexCfg)
{
    DocumentSubDbCollectionInitializer::SP task = std::make_shared<DocumentSubDbCollectionInitializer>();
    for (auto subDb : _subDBs) {
        DocumentSubDbInitializer::SP subTask(subDb->createInitializer(configSnapshot, configSerialNum, indexCfg));
        task->add(subTask);
    }
    return task;
}


void
DocumentSubDBCollection::initViews(const DocumentDBConfig &configSnapshot,
                                   const SessionManager::SP &sessionManager)
{
    for (auto subDb : _subDBs) {
        subDb->initViews(configSnapshot, sessionManager);
    }
}


void
DocumentSubDBCollection::clearViews()
{
    for (auto subDb : _subDBs) {
        subDb->clearViews();
    }
}


void
DocumentSubDBCollection::onReplayDone()
{
    for (auto subDb : _subDBs) {
        subDb->onReplayDone();
    }
}


void
DocumentSubDBCollection::onReprocessDone(SerialNum serialNum)
{
    for (auto subDb : _subDBs) {
        subDb->onReprocessDone(serialNum);
    }
}


SerialNum
DocumentSubDBCollection::getOldestFlushedSerial()
{
    SerialNum lowest = -1;
    for (auto subDb : _subDBs) {
        lowest = std::min(lowest, subDb->getOldestFlushedSerial());
    }
    return lowest;
}


SerialNum
DocumentSubDBCollection::getNewestFlushedSerial()
{
    SerialNum highest = 0;
    for (auto subDb : _subDBs) {
        highest = std::max(highest, subDb->getNewestFlushedSerial());
    }
    return highest;
}


void
DocumentSubDBCollection::pruneRemovedFields(SerialNum serialNum)
{
    for (auto subDb : _subDBs) {
        subDb->pruneRemovedFields(serialNum);
    }
}


void
DocumentSubDBCollection::applyConfig(const DocumentDBConfig &newConfigSnapshot,
                                     const DocumentDBConfig &oldConfigSnapshot,
                                     SerialNum serialNum,
                                     const ReconfigParams &params,
                                     IDocumentDBReferenceResolver &resolver)
{
    _reprocessingRunner.reset();
    for (auto subDb : _subDBs) {
        IReprocessingTask::List tasks;
        tasks = subDb->applyConfig(newConfigSnapshot, oldConfigSnapshot, serialNum, params, resolver);
        _reprocessingRunner.addTasks(tasks);
    }
}

IFeedView::SP
DocumentSubDBCollection::getFeedView()
{
    std::vector<IFeedView::SP> views;
    views.reserve(_subDBs.size());

    for (const auto subDb : _subDBs) {
        views.push_back(subDb->getFeedView());
    }
    IFeedView::SP newFeedView;
    assert( ! views.empty());
    if (views.size() > 1) {
        return std::make_shared<CombiningFeedView>(views, _owner.getBucketSpace(), _calc);
    } else {
        assert(views.front() != nullptr);
        return views.front();
    }
}

IFlushTarget::List
DocumentSubDBCollection::getFlushTargets()
{
    IFlushTarget::List ret;
    for (auto subDb : _subDBs) {
        IFlushTarget::List iTargets(subDb->getFlushTargets());
        ret.insert(ret.end(), iTargets.begin(), iTargets.end());
    }
    return ret;
}

double
DocumentSubDBCollection::getReprocessingProgress() const
{
    return _reprocessingRunner.getProgress();
}

void
DocumentSubDBCollection::close()
{
    for (auto subDb : _subDBs) {
        subDb->close();
    }
}

void
DocumentSubDBCollection::setBucketStateCalculator(const IBucketStateCalculatorSP &calc)
{
    _calc = calc;
    for (auto subDb : _subDBs) {
        subDb->setBucketStateCalculator(calc);
    }
}

void
DocumentSubDBCollection::tearDownReferences(IDocumentDBReferenceResolver &resolver)
{
    for (auto subDb : _subDBs) {
        subDb->tearDownReferences(resolver);
    }
}

} // namespace proton
