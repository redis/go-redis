package entra_id_test

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestEntraId(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EntraId Suite")
}
