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

bool isNull(int i, const uint8_t *nullBitmap) {
    auto nullByte = nullBitmap[i/8];
    return (nullByte&(1<<(i&7)))==0;
}

template<typename T>
std::vector<std::optional<T>>* decodeFixedTypeNullable(const TiDBColumn& column) {
    // gjt todo: delete this!
    auto* result = new std::vector<std::optional<T>>();
    result->reserve(column.length);
    auto nullBitMap = (uint8_t*)(column.nullBitmap);
    auto data = (T*)(column.data);
    for (int i = 0;i < column.length;i++) {
        if (isNull(i, nullBitMap)) {
            result->emplace_back(std::nullopt);
        } else {
            result->emplace_back(data[i]);
        }
    }
    return result;
}

CGoStdVector tidb_chunk_column_to_velox_vector(
        CGoTiDBQueryCtx ctx, void* data, void* nullBitmap,
        void* offsets, size_t length, int type) {
    TiDBColumn tidbColumn;
    tidbColumn.data = data;
    tidbColumn.nullBitmap = nullBitmap;
    tidbColumn.offsets = offsets;
    tidbColumn.length = length;
    if (type == kTiDBColumnTypeInt32 || type == kTiDBColumnTypeInt64) {
        return reinterpret_cast<CGoStdVector>(decodeFixedTypeNullable<int64_t>(tidbColumn));
    } else if (type == kTiDBColumnTypeFloat) {
        return reinterpret_cast<CGoStdVector>(decodeFixedTypeNullable<float>(tidbColumn));
    } else if (type == kTiDBColumnTypeDouble) {
        return reinterpret_cast<CGoStdVector>(decodeFixedTypeNullable<double>(tidbColumn));
    } else {
        std::cout << "InVelox log FATAL ERROR!!! not supported TiDBColumn Type" << std::endl;
        exit(123);
    }
}

template<typename T>
void convertStdVectorToVeloxVector(
        std::shared_ptr<test::VectorMaker> vectorMaker,
        const std::vector<std::optional<T>>& inVec,
        std::vector<VeloxNS::VectorPtr>& out) {
    out.push_back(vectorMaker->flatVectorNullable(inVec, CppToType<T>::create()));
}

// gjt todo: only support nullable.
void enqueue_std_vectors(
        CGoTiDBQueryCtx ctx, const CGoStdVector* vectors,
        const TiDBColumnType* types, size_t num_vec,
        const char* dsID, size_t dsIDLen) {
    const TiDBColumnType* tidbTypes = reinterpret_cast<const TiDBColumnType*>(types);

    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    auto vectorMaker = std::make_shared<test::VectorMaker>(tidbQueryCtx->veloxQueryCtx->pool());

    std::vector<VeloxNS::VectorPtr> results;
    results.reserve(num_vec);
    for (size_t i = 0; i < num_vec; ++i) {
        if (tidbTypes[i] == kTiDBColumnTypeInt32 || tidbTypes[i] == kTiDBColumnTypeInt64) {
            auto* stdVector = reinterpret_cast<const std::vector<std::optional<int64_t>>*>(vectors[i]);
            convertStdVectorToVeloxVector<int64_t>(vectorMaker, *stdVector, results);
        } else if (tidbTypes[i] == kTiDBColumnTypeFloat) {
            auto* stdVector = reinterpret_cast<const std::vector<std::optional<float>>*>(vectors[i]);
            convertStdVectorToVeloxVector<float>(vectorMaker, *stdVector, results);
        } else if (tidbTypes[i] == kTiDBColumnTypeDouble) {
            auto* stdVector = reinterpret_cast<const std::vector<std::optional<double>>*>(vectors[i]);
            convertStdVectorToVeloxVector<double>(vectorMaker, *stdVector, results);
        } else {
            // gjt todo: return error instead of exit(123);
            std::cout << "InVelox log FATAL ERROR!!! not supported TiDBColumn Type" << std::endl;
            exit(123);
        }
    }

    RowVectorPtr rowVector = vectorMaker->rowVector(results);
    const auto& dsIDStr = std::string(dsID, dsIDLen);
    connector::tidb::TiDBDataSource* dataSource = tidbQueryCtx->tidbDataSourceManager->getTiDBDataSource(dsIDStr);
    if (dataSource == nullptr) {
        std::cout << dsIDStr << std::endl;
        exit(123);
    }
    dataSource->enqueue(rowVector);
    dataSource->notifyGotInput();
}

void no_more_input(CGoTiDBQueryCtx ctx, const char* dsID, size_t dsIDLen) {
    auto* tidbQueryCtx = reinterpret_cast<facebook::velox::TiDBQueryCtx*>(ctx);
    std::cout << "InVelox log no more input" << std::endl;
    tidbQueryCtx->noMoreInput = true;

    const auto& dsIDStr = std::string(dsID, dsIDLen);
    connector::tidb::TiDBDataSource* dataSource = tidbQueryCtx->tidbDataSourceManager->getTiDBDataSource(dsIDStr);
    if (dataSource == nullptr) {
        std::cout << dsIDStr << std::endl;
        exit(123);
    }
    dataSource->notifyNoInput();
}

/////
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
    std::vector<std::optional<int32_t>> tmp_col{std::optional<int32_t>{1}};
    VectorPtr vec = vectorMaker->flatVectorNullable(tmp_col , VeloxNS::CppToType<int32_t>::create());
    auto rowVector = vectorMaker->rowVector(std::vector<VectorPtr>{vec});
    std::cout << "InVelox log enqueue_tidb_data_source done" + rowVector->toString(0) << std::endl;
    dataSource->enqueue(rowVector);
}
