extern "C" {
#include <velox/connectors/tidb/tidb_velox_wrapper.h>
}
#include <velox/connectors/tidb/TiDBConnector.h>
#include <vector/tests/utils/VectorMaker.h>
#include <velox/type/Type.h>
#include <memory>
#include <iostream>

namespace ConnectorNS = facebook::velox::connector::tidb;
namespace VeloxNS = facebook::velox;
using namespace facebook::velox;

CGoTiDBDataSource get_tidb_data_source(int64_t id) {
    // TODO: use real table reader id
    ConnectorNS::TiDBDataSource* source = nullptr;
    for (int i = 0; ; ++i) {
        std::cout << "InVelox log get_tidb_data_source beg iter: " + std::to_string(i) << std::endl;
        auto iter = VeloxNS::connector::GetTiDBDataSourceManager().data_sources_.find(0);
        if (iter == VeloxNS::connector::GetTiDBDataSourceManager().data_sources_.end()) {
            if (i < 30) {
                sleep(3);
                continue;
            } else {
                // make sure Velox.TableScan runs first. panic!!
                exit(123);
            }
        } else {
            auto tmpDS = std::dynamic_pointer_cast<ConnectorNS::TiDBDataSource>(iter->second);
            if (tmpDS == nullptr) {
                std::cout << "InVelox log error here1" << std::endl;
                exit(123);
            }
            source = tmpDS.get();
            break;
        }
    }
    std::cout << "InVelox log get_tidb_data_source done" << std::endl;
    return (CGoTiDBDataSource)(source);
}

void enqueue_tidb_data_source(int64_t id, CGoRowVector data) {
    // TODO: use real data
    // VeloxNS::RowVector* tmp = reinterpret_cast<VeloxNS::RowVector*>(data);
    // VeloxNS::RowVectorPtr ptr = VeloxNS::RowVector::createEmpty(tmp->type(), tmp->pool());
    // ptr->copy(tmp, 0, 0, tmp->size());
    // TODO: use tableReader id
    // TODO: make a queryctx for each query
    ::test::VectorMaker* vectorMaker = new ::test::VectorMaker(&(VeloxNS::memory::getProcessDefaultMemoryManager().getRoot()));


    // std::unique_ptr<VeloxNS::memory::MemoryPool> pool{VeloxNS::memory::getDefaultScopedMemoryPool()};
    // ::test::VectorMaker vectorMaker{pool.get()};
    auto tmpDS = VeloxNS::connector::GetTiDBDataSourceManager().getTiDBDataSource(0);
    auto dataSource = std::dynamic_pointer_cast<ConnectorNS::TiDBDataSource>(tmpDS);
    if (dataSource == nullptr) {
        exit(123);
    }
    std::vector<long> tmp_col{1};
    VectorPtr vec = vectorMaker->flatVector(tmp_col , VeloxNS::CppToType<long>::create());
    auto rowVector = vectorMaker->rowVector(std::vector<VectorPtr>{vec});
    std::cout << "InVelox log enqueue_tidb_data_source done" + rowVector->toString(0) << std::endl;
    dataSource->enqueue(rowVector);
}
