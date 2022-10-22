#ifndef TiDB_QUERY_ADAPTER_WRAPPER_
#define TiDB_QUERY_ADAPTER_WRAPPER_

#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct TiDBColumn {
    void* data;
    void* nullBitmap;
    void* offsets;
    size_t length;
} TiDBColumn;

typedef struct TiDBChunk {
    TiDBColumn* columns;
    size_t col_num;
} TiDBChunk;

// CGoTiDBColumn will be used by tidb.
// TiDBColumn will be used in velox.
typedef TiDBColumn CGoTiDBColumn; 
typedef TiDBChunk CGoTiDBChunk;

typedef void* CGoTiDBQueryCtx;
typedef void* CGoRowVector;

CGoTiDBQueryCtx make_tidb_query_ctx();
void make_velox_task_cursor(CGoTiDBQueryCtx ctx, const char* planPB, size_t len);
void fetch_velox_output(CGoTiDBQueryCtx ctx, TiDBColumn** out_cols, size_t* out_col_num);
void destroy_tidb_query_ctx(CGoTiDBQueryCtx ctx);

////////////
void run_tidb_query(char* data, size_t len);
#ifdef __cplusplus
}
#endif

#endif
