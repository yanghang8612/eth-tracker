package model

import "gorm.io/gorm"

const (
	TrackedEthBlockNumKey = "tracked_eth_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
