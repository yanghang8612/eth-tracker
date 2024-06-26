package model

import "gorm.io/gorm"

const (
	TrackedBlockNumKey = "tracked_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
