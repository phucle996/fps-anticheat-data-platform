package handler

import (
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
)

// Handler quản lý tập trung các dependencies cho tất cả HTTP Handlers
type Handler struct {
	cfg       *config.Config
	ipcClient *ipc.Client
}

// NewHandler khởi tạo Handler chứa Config và IPC Client
func NewHandler(cfg *config.Config, ipcClient *ipc.Client) *Handler {
	return &Handler{
		cfg:       cfg,
		ipcClient: ipcClient,
	}
}
