#include <velox/connectors/tidb/TiDBConnector.h>

namespace facebook::velox::connector::tidb {

// gjt todo: global var not work? makeTiDBQueryCtx instead
// VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<TiDBConnectorFactory>())

// TiDBDataSourceManager& GetTiDBDataSourceManager() {
//     static TiDBDataSourceManager gTiDBDataSourceManager;
//     return gTiDBDataSourceManager;
// }
} // namespace facebook::velox::connector::tidb
