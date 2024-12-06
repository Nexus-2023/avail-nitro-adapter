package layeredge

type LayerEdgeConfig struct {
	Enable bool   `koanf:"enable"`
	ApiURL string `koanf:"api-url"`
}

func NewLayerEdgeConfig(enable bool, apiurl string) LayerEdgeConfig {
	return LayerEdgeConfig{
		ApiURL: apiurl,
		Enable: enable,
	}
}
