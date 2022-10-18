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



#endif // VELOX_TIDBCHUNKDECODER_H
