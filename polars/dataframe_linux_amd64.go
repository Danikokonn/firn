//go:build linux && amd64

package polars

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -L${SRCDIR}/../lib -l:libfirn_linux_amd64.a -lm -lpthread -ldl
#include "firn.h"
*/
import "C"
