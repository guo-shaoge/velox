#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CGoTiDBDataSource;
typedef void* CGoRowVector;

CGoTiDBDataSource get_tidb_data_source(int64_t id);

// Life cycle of RowVector should be taken over by Velox.
void enqueue_tidb_data_source(int64_t id, CGoRowVector data);
#ifdef __cplusplus
}
#endif

