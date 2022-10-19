extern "C" {
#include <velox/connectors/tidb/tidb_velox_wrapper.h>
#include <velox/substrait/tidb_query_adapter_wrapper.h>
}
#include <velox/substrait/TiDBQueryAdapter.h>
#include <velox/connectors/tidb/TiDBConnector.h>
#include <vector/tests/utils/VectorMaker.h>
#include <velox/type/Type.h>
#include <memory>
#include <iostream>

namespace ConnectorNS = facebook::velox::connector::tidb;
namespace VeloxNS = facebook::velox;
using namespace facebook::velox;

CGoTiDBDataSource get_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len) {
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    // gjt todo: why wait
    ConnectorNS::TiDBDataSource* dataSource = nullptr;
    for (int i = 0; ; ++i) {
        std::cout << "InVelox log get_tidb_data_source beg iter: " + std::to_string(i) + " " + std::string(dsID, len) << std::endl;
        dataSource = tidbQueryCtx->tidbDataSourceManager->getTiDBDataSource(std::string(dsID, len));
        if (dataSource == nullptr) {
            if (i < 30) {
                sleep(3);
                continue;
            } else {
                // make sure Velox.TableScan runs first. panic!!
                exit(123);
            }
        } else {
            break;
        }
    }
    std::cout << "InVelox log get_tidb_data_source done" << std::endl;
    return (CGoTiDBDataSource)(dataSource);
}

void enqueue_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len, CGoRowVector data) {
    // TODO: use real data
    // VeloxNS::RowVector* tmp = reinterpret_cast<VeloxNS::RowVector*>(data);
    // VeloxNS::RowVectorPtr ptr = VeloxNS::RowVector::createEmpty(tmp->type(), tmp->pool());
    // ptr->copy(tmp, 0, 0, tmp->size());
    // TODO: use tableReader id
    // TODO: make a queryctx for each query
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    std::cout << "InVelox log enqueue_tidb_data_source beg iter: " + std::string(dsID, len) << std::endl;
    ::test::VectorMaker* vectorMaker = new ::test::VectorMaker(&(VeloxNS::memory::getProcessDefaultMemoryManager().getRoot()));

    // std::unique_ptr<VeloxNS::memory::MemoryPool> pool{VeloxNS::memory::getDefaultScopedMemoryPool()};
    // ::test::VectorMaker vectorMaker{pool.get()};
    connector::tidb::TiDBDataSource* dataSource = tidbQueryCtx->tidbDataSourceManager->getTiDBDataSource(std::string(dsID, len));
    if (dataSource == nullptr) {
        exit(123);
    }
    std::vector<std::optional<long>> tmp_col{std::optional<long>{1}};
    VectorPtr vec = vectorMaker->flatVectorNullable(tmp_col , VeloxNS::CppToType<long>::create());
    auto rowVector = vectorMaker->rowVector(std::vector<VectorPtr>{vec});
    std::cout << "InVelox log enqueue_tidb_data_source done" + rowVector->toString(0) << std::endl;
    dataSource->enqueue(rowVector);
}
