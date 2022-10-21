extern "C" {
#include <velox/substrait/tidb_query_adapter_wrapper.h>
}

#include <velox/substrait/TiDBQueryAdapter.h>
#include <velox/substrait/SubstraitToVeloxPlan.h>
#include <velox/exec/tests/utils/Cursor.h>
#include <velox/connectors/tidb/TiDBConnector.h>
#include <velox/common/memory/Memory.h>
#include <iostream>

namespace facebook::velox {

struct InitTiDBQueryAdapter {
    InitTiDBQueryAdapter() {
        bool regTiDBConnectorFactory = connector::registerConnectorFactory((std::make_shared<connector::tidb::TiDBConnectorFactory>()));
        auto tidbConnector = connector::getConnectorFactory(
                connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)
            ->newConnector(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName, nullptr, nullptr);
        connector::registerConnector(tidbConnector);
    }
};

TiDBQueryCtx* makeTiDBQueryCtx() {
    // register things.
    static InitTiDBQueryAdapter inited = InitTiDBQueryAdapter();

    // make queryCtx.
    std::shared_ptr<Config> config = std::make_shared<core::MemConfig>();
    std::shared_ptr<folly::Executor> executor =
        std::make_shared<folly::CPUThreadPoolExecutor>(
                std::thread::hardware_concurrency());
    auto queryCtx = std::make_shared<core::QueryCtx>(std::move(executor), std::move(config));

    // make TiDBDataSourceManager.
    // gjt todo: make const
    auto mgr = std::make_shared<connector::tidb::TiDBDataSourceManager>();

    TiDBQueryCtx* tidbQueryCtx = new TiDBQueryCtx();
    tidbQueryCtx->veloxQueryCtx = queryCtx;
    tidbQueryCtx->tidbDataSourceManager = mgr;
    tidbQueryCtx->noMoreInput = false;
    return tidbQueryCtx;
}

std::shared_ptr<exec::test::TaskCursor> makeVeloxTaskCursor(TiDBQueryCtx* tidbQueryCtx, const std::string& planPB) {
    ::substrait::Plan substraitPlan;
    substraitPlan.ParseFromString(planPB);
    std::cout << "InVelox log substraitPlan.DebugString(): " << substraitPlan.DebugString() << std::endl;

    substrait::SubstraitVeloxPlanConverter planConverter;
    planConverter.setTiDBDataSourceManager(tidbQueryCtx->tidbDataSourceManager);
    auto planNode = planConverter.toVeloxPlan(substraitPlan, tidbQueryCtx->veloxQueryCtx->pool());
    tidbQueryCtx->veloxPlanNode = planNode;

    facebook::velox::exec::test::CursorParameters param;
    param.queryCtx = tidbQueryCtx->veloxQueryCtx;
    // GJT Watchout: 1 thread
    param.maxDrivers = 1;
    param.planNode = planNode;
    tidbQueryCtx->veloxPlanNode = planNode;

    std::cout << "InVelox log start create TaskCursor" << std::endl;
    auto cursor = std::make_shared<facebook::velox::exec::test::TaskCursor>(param);
    auto* task = cursor->task().get();
    // std::unordered_set<core::PlanNodeId> leafPlanNodeIds = tidbQueryCtx->veloxPlanNode->leafPlanNodeIds();
    task->addSplit(tidbQueryCtx->veloxPlanNode->id(),
            exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
    // task->noMoreSplits(tidbQueryCtx->veloxPlanNode->id());
    cursor->start();

    return cursor;
}

// gjt todo: all memory are newed, check memory release. Better way is to copy data to go directly.
template<typename T>
TiDBColumn* veloxVecToTiDBColumn(const FlatVector<T>& flatVec) {
    T* data = new T[flatVec.size()]();
    char* nulls = new char[flatVec.size()/8 + 1];
    for (vector_size_t j = 0; j < flatVec.size(); ++j) {
        bits::setNull(reinterpret_cast<uint64_t*>(nulls), j, flatVec.isNullAt(j));
    }
    for (vector_size_t j = 0; j < flatVec.size(); ++j) {
        data[j] = flatVec.valueAt(j);
    }
    auto* col = new TiDBColumn();
    col->data = data;
    col->nullBitmap = nulls;
    col->offsets = nullptr;
    col->length = static_cast<size_t>(flatVec.size());
    return col;
}

TiDBChunk* fetchVeloxOutput(facebook::velox::TiDBQueryCtx* tidbQueryCtx) {
    std::cout << "InVelox log start fetch velox output" << std::endl;

    auto cursor = tidbQueryCtx->veloxTaskCursor;
    auto* task = cursor->task().get();
    if (!tidbQueryCtx->noMoreInput) {
        task->addSplit(tidbQueryCtx->veloxPlanNode->id(),
                exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
        std::cout << "InVelox log start fetch velox output split added" << std::endl;
    } else {
        task->noMoreSplits(tidbQueryCtx->veloxPlanNode->id());
        std::cout << "InVelox log start fetch velox output nosplit added" << std::endl;
    }
    // if (!cursor->moveNext() || tidbQueryCtx->noMoreInput) {
    //     std::cout << "InVelox log fetchVeloxOutput rowVec null, no more input" << std::endl;
    //     return nullptr;
    // }
    bool moveSucc = cursor->moveNext();
    if (!moveSucc) {
        std::cout << "InVelox log fetchVeloxOutput moveNext: " << moveSucc << std::endl;
        return nullptr;
    }
    RowVectorPtr rowVec = cursor->current();
    for (size_t i = 0; i < rowVec->size(); ++i) {
        std::cout << "InVelox log result: " + rowVec->toString(vector_size_t(i)) << std::endl;
    }
    // std::cout << "InVelox log fetch velox output clear" << std::endl;
    // while (cursor->moveNext()) {}
    std::cout << "InVelox log fetch velox output done" << std::endl;

    // gjt todo: check memory release.
    TiDBColumn** columns = new TiDBColumn*[rowVec->childrenSize()];
    for (size_t i = 0; i < rowVec->childrenSize(); ++i) {
        std::cout << "InVelox log handle " << std::to_string(i) << " column begin" << std::endl;
        auto& child = rowVec->childAt(i);
        auto k = child->type()->kind();
        if (k == TypeKind::INTEGER) {
            columns[i] = veloxVecToTiDBColumn(*(child->asFlatVector<int32_t>()));
        } else if (k == TypeKind::BIGINT) {
            columns[i] = veloxVecToTiDBColumn(*(child->asFlatVector<int64_t>()));
        } else if (k == TypeKind::REAL) {
            columns[i] = veloxVecToTiDBColumn(*(child->asFlatVector<float>()));
        } else if (k == TypeKind::DOUBLE) {
            columns[i] = veloxVecToTiDBColumn(*(child->asFlatVector<double>()));
        } else {
            std::cout << "InVelox log doesn't support this type" << std::endl;
            exit(456);
        }
        std::cout << "InVelox log handle " << std::to_string(i) << " column done" << std::endl;
    }
    auto* chunk = new TiDBChunk{.columns = columns, .col_num = rowVec->childrenSize()};
    return chunk;
}

} // namespace facebook::velox

CGoTiDBQueryCtx make_tidb_query_ctx() {
    auto* tidbQueryCtx = facebook::velox::makeTiDBQueryCtx();
    return reinterpret_cast<void*>(tidbQueryCtx);
}

void make_velox_task_cursor(CGoTiDBQueryCtx ctx, const char* planPB, size_t len) {
    facebook::velox::TiDBQueryCtx* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    auto cursor = facebook::velox::makeVeloxTaskCursor(tidbQueryCtx, std::string(planPB, len));
    tidbQueryCtx->veloxTaskCursor = cursor;
}

void fetch_velox_output(CGoTiDBQueryCtx ctx, TiDBColumn** out_cols, size_t* out_col_num) {
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    TiDBChunk* res = facebook::velox::fetchVeloxOutput(tidbQueryCtx);
    if (res != nullptr) {
        *out_cols = *(res->columns);
        *out_col_num = res->col_num;
    } else {
        *out_col_num = 0;
    }
    std::cout << "InVelox log  fetch_velox_output done" << std::endl;
}

void destroy_tidb_query_ctx(CGoTiDBQueryCtx ctx) {
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    tidbQueryCtx->veloxQueryCtx.reset();
    tidbQueryCtx->veloxTaskCursor.reset();
    tidbQueryCtx->veloxPlanNode.reset();
}

/*
 * Basically same as tests/Substrait2VeloxPlanConversionTest. Do:
 * 1. Receive Substrait PB Plan
 * 2. Construct Task
 * 3. Get Velox output and push to Queue
 */

// namespace facebook::velox::substrait {
// void runTiDBQuery(const std::string& planPB) {
//     // Register everything.
//     static bool regTiDBConnectorFactory = connector::registerConnectorFactory((std::make_shared<connector::tidb::TiDBConnectorFactory>()));
//     static auto& setupTiDBDataSourceManager = connector::GetTiDBDataSourceManager();
// 
//     auto tidbConnector = connector::getConnectorFactory(
//             connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)
//         ->newConnector(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName, nullptr, nullptr);
//     connector::registerConnector(tidbConnector);
// 
//     // Parse PlanPB.
//     ::substrait::Plan substraitPlan;
//     substraitPlan.ParseFromString(planPB);
//     std::cout << "InVelox log substraitPlan.DebugString(): " << substraitPlan.DebugString() << std::endl;
// 
//     SubstraitVeloxPlanConverter planConverter;
//     std::shared_ptr<Config> config = std::make_shared<facebook::velox::core::MemConfig>();
//     std::shared_ptr<folly::Executor> executor =
//         std::make_shared<folly::CPUThreadPoolExecutor>(
//                 std::thread::hardware_concurrency());
//     auto queryCtx = std::make_shared<facebook::velox::core::QueryCtx>(std::move(executor), std::move(config));
// 
//     std::cout << "InVelox log start toVeloxPlan" << std::endl;
//     auto planNode = planConverter.toVeloxPlan(substraitPlan, queryCtx->pool());
// 
//     // Create TaskCursor.
//     facebook::velox::exec::test::CursorParameters param;
//     param.queryCtx = queryCtx;
//     param.maxDrivers = 1;
//     param.planNode = planNode;
// 
//     // Add Split and get Velox output.
//     std::cout << "InVelox log start create TaskCursor" << std::endl;
//     auto cursor = std::make_shared<facebook::velox::exec::test::TaskCursor>(param);
//     auto* task = cursor->task().get();
//     task->addSplit(planNode->id(), exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
//     while (cursor->moveNext()) {
//         RowVectorPtr vec = cursor->current();
//         task->addSplit(planNode->id(), exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
//         for (size_t i = 0; i < vec->size(); ++i) {
//             std::cout << "InVelox log result: " + vec->toString(vector_size_t(i)) << std::endl;
//         }
//     }
// }
// } // namespace::velox::substrait

// // CGo wrapper
// void run_tidb_query(char* data, size_t len) {
//     std::string planPB(data, len);
//     facebook::velox::substrait::runTiDBQuery(planPB);
// }
