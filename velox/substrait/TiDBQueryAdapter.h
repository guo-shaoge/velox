#include <velox/connectors/tidb/TiDBConnector.h>
#include <velox/exec/tests/utils/Cursor.h>

namespace facebook::velox {
// gjt todo: only one task for each query?
struct TiDBQueryCtx {
    std::shared_ptr<core::QueryCtx> veloxQueryCtx;
    std::shared_ptr<exec::test::TaskCursor> veloxTaskCursor;
    std::shared_ptr<const core::PlanNode> veloxPlanNode;
    std::shared_ptr<connector::tidb::TiDBDataSourceManager> tidbDataSourceManager;
    bool noMoreInput;
};
} // namespace facebook::velox
