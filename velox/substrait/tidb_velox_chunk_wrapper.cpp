//
// Created by Shenghui Wu on 2022/10/19.
//

#include "tidb_velox_chunk_wrapper.h"
#include "TiDBChunkDecoder.h"


int testColumn(void* data, void* nullBitmap, void* offsets, long long length,
               void* data2, void* nullBitmap2, void* offsets2, void* length2) {
  fprintf(stderr,"decode\n");
  TiDBColumn col{
      data,
      nullBitmap,
      offsets,
      length};
  for (int i = 0;i < 10;i++) {
    std::cerr << ((int64_t*)data)[i] << std::endl;
  }
  TiDBColumnDecoder decoder;
  auto vec = decoder.ColumnToVector(col);
  auto flat_vec = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec);
  for (int i = 0;i < 10;i++) {
    std::cerr << flat_vec->valueAt(i) << std::endl;
  }
  fprintf(stderr,"encode\n");
  TiDBColumnEncoder encoder;
  TiDBColumn col2 = encoder.baseVectorToColumn(vec);
  *(void**)data2 = col2.data;
  for (int i = 0;i < 10;i++) {
    std::cerr << ((int64_t*)*(void**)data2)[i] << std::endl;
  }
  *(void**)nullBitmap2 = col2.nullBitmap;
  *(void**)offsets2 = col2.offsets;
  *(long long*)length2 = col2.length;
  return 555;
};

