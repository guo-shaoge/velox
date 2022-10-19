#include <stdint.h>
#include <stddef.h>
#include <velox/substrait/tidb_query_adapter_wrapper.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CGoTiDBDataSource;
typedef void* CGoRowVector;

CGoTiDBDataSource get_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len);

// Life cycle of RowVector should be taken over by Velox.
void enqueue_tidb_data_source(CGoTiDBQueryCtx ctx, const char* dsID, size_t len, CGoRowVector data);
#ifdef __cplusplus
}
#endif

