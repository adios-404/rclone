//go:build !linux && !(darwin && cgo)

package fswatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsupported(t *testing.T) {
	assert.False(t, Supported())
	err := Watch(context.Background(), t.TempDir(), Options{}, func([]Event) {})
	assert.ErrorIs(t, err, ErrUnsupported)
}
