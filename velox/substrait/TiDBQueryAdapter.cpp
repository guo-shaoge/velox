extern "C" {
#include <velox/substrait/tidb_query_adapter_wrapper.h>
}
#include <velox/substrait/SubstraitToVeloxPlan.h>
#include <velox/exec/tests/utils/Cursor.h>
#include <velox/connectors/tidb/TiDBConnector.h>
#include <velox/common/memory/Memory.h>
#include <iostream>

namespace facebook::velox {
struct TiDBQueryCtx {
    std::shared_ptr<core::QueryCtx> veloxQueryCtx;
    std::shared_ptr<exec::test::TaskCursor> veloxTaskCursor;
    std::shared_ptr<const core::PlanNode> veloxPlanNode;
};

TiDBQueryCtx* makeTiDBQueryCtx() {
    // register things.
    static bool regTiDBConnectorFactory = connector::registerConnectorFactory((std::make_shared<connector::tidb::TiDBConnectorFactory>()));
    static auto& setupTiDBDataSourceManager = connector::GetTiDBDataSourceManager();
    auto tidbConnector = connector::getConnectorFactory(
            connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)
        ->newConnector(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName, nullptr, nullptr);
    connector::registerConnector(tidbConnector);

    std::shared_ptr<Config> config = std::make_shared<core::MemConfig>();
    std::shared_ptr<folly::Executor> executor =
        std::make_shared<folly::CPUThreadPoolExecutor>(
                std::thread::hardware_concurrency());
    auto queryCtx = std::make_shared<core::QueryCtx>(std::move(executor), std::move(config));

    TiDBQueryCtx* tidbQueryCtx = new TiDBQueryCtx();
    tidbQueryCtx->veloxQueryCtx = queryCtx;
    return tidbQueryCtx;
}

std::shared_ptr<exec::test::TaskCursor> makeVeloxTaskCursor(TiDBQueryCtx* tidbQueryCtx, const std::string& planPB) {
    ::substrait::Plan substraitPlan;
    substraitPlan.ParseFromString(planPB);
    std::cout << "InVelox log substraitPlan.DebugString(): " << substraitPlan.DebugString() << std::endl;

    substrait::SubstraitVeloxPlanConverter planConverter;
    auto planNode = planConverter.toVeloxPlan(substraitPlan, tidbQueryCtx->veloxQueryCtx->pool());

    facebook::velox::exec::test::CursorParameters param;
    param.queryCtx = tidbQueryCtx->veloxQueryCtx;
    // GJT Watchout: 1 thread
    param.maxDrivers = 1;
    param.planNode = planNode;
    tidbQueryCtx->veloxPlanNode = planNode;

    std::cout << "InVelox log start create TaskCursor" << std::endl;
    auto cursor = std::make_shared<facebook::velox::exec::test::TaskCursor>(param);
    auto* task = cursor->task().get();
    task->addSplit(tidbQueryCtx->veloxPlanNode->id(),
            exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
    cursor->start();

    return cursor;
}

RowVectorPtr fetchVeloxOutput(facebook::velox::TiDBQueryCtx* tidbQueryCtx) {
    std::cout << "InVelox log start fetch velox output" << std::endl;
    auto cursor = tidbQueryCtx->veloxTaskCursor;
    auto* task = cursor->task().get();
    task->addSplit(tidbQueryCtx->veloxPlanNode->id(),
            exec::Split(std::make_shared<connector::tidb::TiDBConnectorSplit>(connector::tidb::TiDBConnectorFactory::kTiDBConnectorName)));
    cursor->moveNext();
    RowVectorPtr res = cursor->current();
    for (size_t i = 0; i < res->size(); ++i) {
        std::cout << "InVelox log result: " + res->toString(vector_size_t(i)) << std::endl;
    }
    std::cout << "InVelox log fetch velox output done" << std::endl;
    return res;
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

CGoRowVector fetch_velox_output(CGoTiDBQueryCtx ctx) {
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    auto res = facebook::velox::fetchVeloxOutput(tidbQueryCtx);
    // TODO here
    return nullptr;
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
