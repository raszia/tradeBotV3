package redis

import (
	"testing"

	"v3TradeBot/internal/config"
)

func TestBuildOptionsDefaults(t *testing.T) {
	opts := buildOptions(config.RedisConfig{Addr: "127.0.0.1:6379"})
	if opts.PoolSize != defaultPoolSize {
		t.Errorf("PoolSize = %d, want default %d", opts.PoolSize, defaultPoolSize)
	}
	if opts.MinIdleConns != defaultMinIdleConns {
		t.Errorf("MinIdleConns = %d, want %d", opts.MinIdleConns, defaultMinIdleConns)
	}
	if opts.ConnMaxIdleTime != defaultIdleTimeout {
		t.Errorf("ConnMaxIdleTime = %v, want %v", opts.ConnMaxIdleTime, defaultIdleTimeout)
	}
	if opts.ConnMaxLifetime != defaultMaxLifetime {
		t.Errorf("ConnMaxLifetime = %v, want %v", opts.ConnMaxLifetime, defaultMaxLifetime)
	}
}

func TestBuildOptionsRespectsOverrides(t *testing.T) {
	opts := buildOptions(config.RedisConfig{
		Addr:     "redis:6380",
		Password: "pw",
		DB:       3,
		PoolSize: 7,
	})
	if opts.Addr != "redis:6380" {
		t.Errorf("Addr = %q", opts.Addr)
	}
	if opts.Password != "pw" {
		t.Errorf("Password = %q", opts.Password)
	}
	if opts.DB != 3 {
		t.Errorf("DB = %d", opts.DB)
	}
	if opts.PoolSize != 7 {
		t.Errorf("PoolSize = %d, want override 7", opts.PoolSize)
	}
}
