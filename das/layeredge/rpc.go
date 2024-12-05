package layeredge

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ethereum/go-ethereum/log"
	"github.com/go-resty/resty/v2"
)

type LayerEdgeWriter interface {
	AddBlockByNumber(ctx context.Context, blockNumber uint32) error
}

type layerEdgeRPC struct {
	client *resty.Client
	url    string
}

func (l *layerEdgeRPC) AddBlockByNumber(ctx context.Context, blockNumber uint32) error {
	modifiedPath, err := url.JoinPath(l.url, "/add-block-by-number/{block_number}")
	if err != nil {
		log.Error("error in joining path API", "error", err)
		return err
	}

	resp, err := l.client.R().
		SetPathParam("block_number", strconv.Itoa(int(blockNumber))).
		Get(modifiedPath)
	if err != nil {
		log.Error("error in calling AddBlockByNumber API", "error", err)
		return err
	}

	responseBody := resp.Body()
	log.Info("msg successfully posted response", "body", string(responseBody))

	return nil
}

func NewLayerEdgeWriter(cfg LayerEdgeConfig) (LayerEdgeWriter, error) {
	_, err := url.ParseRequestURI(cfg.ApiURL)
	if err != nil {
		log.Error("not valid LayerEdge BaseURL", "error", err)
		return nil, err
	}

	client := resty.New()

	client = client.OnAfterResponse(func(c *resty.Client, r *resty.Response) error {
		if r.StatusCode() != http.StatusOK {
			log.Error(
				"status not ok", "status", r.Status(),
				"body", string(r.Body()),
			)
			return fmt.Errorf("STATUS NOT OK")
		}

		return nil
	})

	l := &layerEdgeRPC{
		client: client,
		url:    cfg.ApiURL,
	}

	return l, nil
}
