#include <stdint.h>
#include <stddef.h>
#include <velox/substrait/tidb_query_adapter_wrapper.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CGoTiDBDataSource;
typedef void* CGoRowVector;

typedef void* CGoStdVector;
typedef void* CGoTiDBColumnType;
typedef int TiDBColumnType;
const TiDBColumnType kTiDBColumnTypeInt32 = 0;
const TiDBColumnType kTiDBColumnTypeInt64 = 1;
const TiDBColumnType kTiDBColumnTypeFloat = 2;
const TiDBColumnType kTiDBColumnTypeDouble = 3;

CGoTiDBDataSource get_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len);

CGoStdVector tidb_chunk_column_to_velox_vector(
        CGoTiDBQueryCtx ctx, void* data, void* nullBitmap, void* offsets, size_t length, int type);
void enqueue_std_vectors(
        CGoTiDBQueryCtx ctx, const CGoStdVector* vectors,
        const TiDBColumnType* types, size_t num_vec,
        const char* dsID, size_t dsIDLen);

/////// gjt todo: delete following
// Life cycle of RowVector should be taken over by Velox.
void enqueue_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len, CGoRowVector data);
#ifdef __cplusplus
}
#endif

