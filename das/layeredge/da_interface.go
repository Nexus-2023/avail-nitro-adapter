package layeredge

import (
	"context"

	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbstate/daprovider"
	"github.com/offchainlabs/nitro/das/avail"
)

type layerEdgeClientImpl struct {
	rpc         LayerEdgeWriter
	availWriter daprovider.Writer
}

func (l *layerEdgeClientImpl) Store(ctx context.Context, message []byte,
	timeout uint64, disableFallbackStoreDataOnChain bool) ([]byte, error) {
	data, err := l.availWriter.Store(ctx, message, timeout, disableFallbackStoreDataOnChain)
	if err != nil {
		log.Error("error in storing data to avail", "error", err)
		return nil, err
	}

	blobData := new(avail.BlobPointer)
	// Need to skip one byte as it is padded using avail header
	err = blobData.UnmarshalFromBinary(data[1:])
	if err != nil {
		log.Error("error in unmarshalling blob data", "error", err)
		return nil, err
	}

	err = l.rpc.AddBlockByNumber(ctx, blobData.BlockHeight)
	if err != nil {
		return nil, err
	}

	return data, err
}

func NewLayerEdgeDAWriter(layerEdgeWriter LayerEdgeWriter, availWriter daprovider.Writer) *layerEdgeClientImpl {
	l := &layerEdgeClientImpl{
		rpc:         layerEdgeWriter,
		availWriter: availWriter,
	}

	return l
}
