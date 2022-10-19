package tidb_velox_wrapper

//#cgo CFLAGS: -I/home/guojiangtao/code/velox
//#cgo LDFLAGS: -L${SRCDIR} -ltidb_velox
//
//#include <velox/connectors/tidb/tidb_velox_wrapper.h>
import "C"
import "unsafe"

type CGoVeloxDataSource struct {
	tidbDataSource *C.CGoTiDBDataSource
	id int64
}

func NewCGoVeloxDataSource(id int64) *CGoVeloxDataSource {
	C.CGoTiDBDataSource s = C.get_tidb_data_source(id)
	return &CGoVeloxDataSource {
		tidbDataSource: &s,
		id: id,
	}
}

func (s *CGoVeloxDataSource) enqueue(C.CGoRowVector data) {
	C.enqueue_tidb_data_source(C.long(id), unsafe.Pointer(data))
}

func (s *CGoVeloxDataSource) destroy() {
}
