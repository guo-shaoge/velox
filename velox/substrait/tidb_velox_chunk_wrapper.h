//
// Created by Shenghui Wu on 2022/10/19.
//

#ifndef VELOX_TIDB_VELOX_CHUNK_WRAPPER_H
#define VELOX_TIDB_VELOX_CHUNK_WRAPPER_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {

#endif

int testColumn(void* data, void* nullBitmap, void* offsets, long long length,
               void* data2, void* nullBitmap2, void* offsets2, void* length2) ;

// Life cycle of RowVector should be taken over by Velox.

#ifdef __cplusplus
}
#endif

#endif // VELOX_TIDB_VELOX_CHUNK_WRAPPER_H
