package avail

import (
	"time"
)

type DAConfig struct {
	Enable  bool          `koanf:"enable"`
	ApiURL  string        `koanf:"api-url"`
	Seed    string        `koanf:"seed"`
	AppID   int           `koanf:"app-id"`
	Timeout time.Duration `koanf:"timeout"`
	VectorX string        `koanf:"vectorx"`
}

func NewDAConfig(api_url string, seed string, app_id int, timeout time.Duration, vectorx string) (*DAConfig, error) {
	return &DAConfig{
		Enable:  true,
		ApiURL:  api_url,
		Seed:    seed,
		AppID:   app_id,
		Timeout: timeout,
		VectorX: vectorx,
	}, nil
}
