#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef void* CGoTiDBQueryCtx;
typedef void* CGoRowVector;

CGoTiDBQueryCtx make_tidb_query_ctx();
void make_velox_task_cursor(CGoTiDBQueryCtx ctx, const char* planPB, size_t len);
CGoRowVector fetch_velox_output(CGoTiDBQueryCtx ctx);
void destroy_tidb_query_ctx(CGoTiDBQueryCtx ctx);

////////////
void run_tidb_query(char* data, size_t len);
#ifdef __cplusplus
}
#endif
