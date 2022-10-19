//
// Created by Shenghui Wu on 2022/10/18.
//

#ifndef VELOX_TIDBCHUNKDECODER_H
#define VELOX_TIDBCHUNKDECODER_H

#include "velox/substrait/tests/JsonToProtoConverter.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

struct TiDBColumn {
  int64_t *data;
  int64_t length;
  int64_t *nullBitmap;
  // int64_t offsets;
};

struct TiDBChunk {
    TiDBColumn *columns;
    int64_t size;
};

class TiDBColumnDecoder {
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  ::test::VectorMaker vectorMaker_{pool_.get()};

  static inline bool isNull(int i, const int64_t *nullBitmap) {
    auto nullByte = nullBitmap[i/8];
    return (nullByte&(1<<(i&7)))==0;
  }

  static std::vector<std::optional<int64_t>> decodeInts(TiDBColumn column) {
    std::vector<std::optional<int64_t>> result;
    result.reserve(column.length);
    for (int i = 0;i < column.length;i++) {
      if (isNull(i, column.nullBitmap)) {
        result.emplace_back(std::nullopt);
      } else {
        result.emplace_back(column.data[i]);
      }
    }
    return std::move(result);
  }

  public:
    VectorPtr ColumnToVector(TiDBColumn column) {
    auto result = decodeInts(column);
    return vectorMaker_.flatVectorNullable(result,CppToType<int64_t>::create());
  }
};

class TiDBChunkDecoder {
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  ::test::VectorMaker vectorMaker_{pool_.get()};
  TiDBColumnDecoder columnDecoder;

  RowVectorPtr ChunkToRowVector(TiDBChunk chunk) {
    std::vector<VectorPtr> results;
    results.reserve(chunk.size);
    for (int i = 0;i < chunk.size;i++) {
      auto oneCol = columnDecoder.ColumnToVector(chunk.columns[i]);
      results.emplace_back(oneCol);
    }
    return vectorMaker_.rowVector(results);
  }
};

class TiDBColumnEncoder {
 private:
  static void setNull(int i, int64_t* nullBitmap) {
    nullBitmap[i>>3] &= ~(1 << uint(i&7));
  }

 public:
  static TiDBColumn baseVectorToColumn(const VectorPtr& vec){
    TiDBColumn col{};
    col.length = vec->size();
    col.nullBitmap = (int64_t *)malloc(sizeof(int64_t)*(col.length+7)/8);
    col.data = (int64_t *)malloc(sizeof(int64_t)*col.length);
    memset(col.nullBitmap, -1, sizeof(int64_t)*(col.length+7)/8);
    memset(col.data, 0, sizeof(int64_t)*col.length);

    auto flatVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec);
    for (int i = 0;i < col.length;i++) {
      if (vec->isNullAt(i)) {
        setNull(i, col.nullBitmap);
      } else {
        col.data[i] = flatVec->valueAt(i);
      }
    }
    return col;
  }
};

class TiDBChunkEncoder{
  TiDBColumnEncoder columnEncoder;
  TiDBChunk RowVectorToChunk(const RowVectorPtr& row) {
    TiDBChunk res{};
    res.size = (int64_t)row->childrenSize();
    res.columns = (TiDBColumn*)malloc(sizeof(TiDBColumn)*res.size);
    for (int i = 0;i < res.size;i++) {
      res.columns[i] = columnEncoder.baseVectorToColumn(row->childAt(i));
    }
    return res;
  }
};


#endif // VELOX_TIDBCHUNKDECODER_H
