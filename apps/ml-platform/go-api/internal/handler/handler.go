package handler

import (
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/service"
)

// Handler quản lý HTTP Transport Layer cho Gin Web Framework
type Handler struct {
	cfg      *config.Config
	services *service.ServiceContainer
}

// NewHandler khởi tạo Transport Handler chứa Config và Services Container
func NewHandler(cfg *config.Config, services *service.ServiceContainer) *Handler {
	return &Handler{
		cfg:      cfg,
		services: services,
	}
}
